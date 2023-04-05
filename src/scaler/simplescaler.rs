use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;

use crate::{
    commands::{Command, StartActor, StopActor},
    events::Event,
    scaler::Scaler,
    storage::{Actor, Host, ReadStore},
};

/// Config for a SimpleActorScaler, which ensures that an actor as referenced by
/// `actor_reference` in lattice `lattice_id` runs with `replicas` replicas
struct SimpleScalerConfig {
    // Sourced from the component `image` property
    actor_reference: String,
    // Sourced from the lattice that the scaler is configured on
    lattice_id: String,
    // Required configuration in the `simplescaler` block
    replicas: usize,
}

/// The SimpleScaler ensures that a certain number of replicas are running
/// for a certain public key.
///
/// This is primarily to demonstrate the functionality and ergonomics of the
/// [Scaler](crate::scaler::Scaler) trait and doesn't make any guarantees
/// about spreading replicas evenly
struct SimpleActorScaler<S: ReadStore + Send + Sync> {
    pub config: SimpleScalerConfig,
    store: S,
}

#[async_trait]
impl<S: ReadStore + Send + Sync> Scaler for SimpleActorScaler<S> {
    type Config = SimpleScalerConfig;

    fn update_config(&mut self, config: Self::Config) -> Result<bool> {
        self.config = config;
        Ok(true)
    }

    async fn handle_event(&self, event: Event) -> Result<HashSet<Command>> {
        match event {
            Event::ActorStarted(_) | Event::ActorStopped(_) | Event::HostStopped(_) => {
                self.compute_actor_commands(&self.store).await
            }
            // No other event impacts the job of this scaler so we can ignore it
            _ => Ok(HashSet::new()),
        }
    }

    async fn reconcile(&self) -> Result<HashSet<Command>> {
        self.compute_actor_commands(&self.store).await
    }
}

impl<S: ReadStore + Send + Sync> SimpleActorScaler<S> {
    #[allow(unused)]
    /// Construct a new SimpleActorScaler with specified configuration values
    fn new(store: S, actor_reference: String, lattice_id: String, replicas: usize) -> Self {
        Self {
            store,
            config: SimpleScalerConfig {
                actor_reference,
                lattice_id,
                replicas,
            },
        }
    }

    /// Given a readable store containing the state of the lattice, compute the
    /// required commands to either stop extra actor instances or start new
    /// actor instances to reach the configured replica count
    async fn compute_actor_commands(&self, store: &S) -> Result<HashSet<Command>> {
        // NOTE(brooksmtownsend): This will fail to look up the actor ID if an actor is not running in the lattice currently.
        // This is acceptable for the simplescaler but might require a helper function in the future
        let actor_id = store
            .list::<Actor>(&self.config.lattice_id)
            .await?
            .iter()
            .find(|(_id, actor)| actor.reference == self.config.actor_reference)
            .map(|(id, _actor)| id.to_owned())
            // Default here means the below `get` will find zero running actors, which is fine because
            // that accurately describes the current lattice having zero instances.
            .unwrap_or_default();

        Ok(
            match store
                .get::<Actor>(&self.config.lattice_id, &actor_id)
                .await?
            {
                Some(actors) => {
                    // NOTE(brooksmtownsend): This should ideally take into account the annotations to ensure the actors
                    // we're comparing against are wadm managed actors
                    let count = self.config.replicas as i16 - actors.count() as i16;

                    // It doesn't read cleaner to do this in a comparison chain
                    #[allow(clippy::comparison_chain)]
                    if count > 0 {
                        // Choosing to retrieve the first host that an actor is running on over querying the store for efficiency
                        let host_id = actors.count.keys().next().cloned().unwrap_or_default();

                        HashSet::from_iter([Command::StartActor(StartActor {
                            reference: self.config.actor_reference.to_owned(),
                            count: count as usize, // It's a positive integer so we know this will succeed
                            host_id,
                        })])
                    } else if count < 0 {
                        // This is written iteratively rather than functionally just because it reads better.
                        let mut remaining = count.unsigned_abs() as usize;
                        let mut commands = HashSet::new();

                        // For each host running this actor, request actor stops until
                        // the total number of stops equals the number of extra instances
                        for (host_id, count) in actors.count {
                            if remaining == 0 {
                                break;
                            } else if remaining >= count {
                                commands.insert(Command::StopActor(StopActor {
                                    actor_id: actor_id.to_owned(),
                                    host_id,
                                    count,
                                }));
                                remaining -= count;
                            } else {
                                commands.insert(Command::StopActor(StopActor {
                                    actor_id: actor_id.to_owned(),
                                    host_id,
                                    count: remaining,
                                }));
                                remaining = 0;
                            }
                        }

                        commands
                    } else {
                        HashSet::new()
                    }
                }
                None => {
                    if let Some(host_id) = store
                        .list::<Host>(&self.config.lattice_id)
                        .await?
                        .iter()
                        .next()
                        .map(|(host_id, _host)| host_id)
                    {
                        HashSet::from_iter([Command::StartActor(StartActor {
                            reference: self.config.actor_reference.to_owned(),
                            count: self.config.replicas,
                            host_id: host_id.to_owned(),
                        })])
                    } else {
                        return Err(anyhow::anyhow!(
                            "No hosts running, unable to return actor start commands"
                        ));
                    }
                }
            },
        )
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use crate::{
        commands::{Command, StartActor},
        consumers::{manager::Worker, ScopedMessage},
        events::{ActorClaims, ActorStarted, Event, HostStarted},
        scaler::{simplescaler::SimpleActorScaler, Scaler},
        test_util::TestStore,
        workers::EventWorker,
    };

    #[tokio::test]
    async fn can_return_error_with_no_hosts() {
        let lattice_id = "hoohah_no_host";
        let actor_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let replicas = 12;

        let store = Arc::new(TestStore::default());
        let simple_scaler = SimpleActorScaler::new(
            store.clone(),
            actor_reference,
            lattice_id.to_string(),
            replicas,
        );

        let cmds = simple_scaler.reconcile().await;
        assert!(cmds.is_err());
        assert_eq!(
            cmds.unwrap_err().to_string(),
            "No hosts running, unable to return actor start commands".to_string()
        );
    }

    #[tokio::test]
    async fn can_request_start_actor() {
        let lattice_id = "hoohah_start_actor";
        let actor_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let replicas = 12;

        // *** STATE SETUP BEGIN ***
        // Lattice State: One empty host

        let store = Arc::new(TestStore::default());
        let worker = EventWorker::new(store.clone(), HashMap::default());

        let host_id = "NASDASDIAMAREALHOST".to_string();
        let host_name = "I am a real host".to_string();

        let labels = HashMap::from([("real".to_string(), "true".to_string())]);

        worker
            .do_work(ScopedMessage::<Event> {
                lattice_id: lattice_id.to_string(),
                inner: Event::HostStarted(HostStarted {
                    labels,
                    friendly_name: host_name,
                    id: host_id.to_string(),
                }),
                acker: None,
            })
            .await
            .expect("Should be able to handle the host started event");
        // *** STATE SETUP END ***
        // Expected Actions: Start 12 replicas of the actor on the one host

        let simple_scaler = SimpleActorScaler::new(
            store.clone(),
            actor_reference.to_string(),
            lattice_id.to_string(),
            replicas,
        );

        let cmds = simple_scaler
            .reconcile()
            .await
            .expect("Should have computed a set of commands");
        assert_eq!(cmds.len(), 1);
        let command = cmds
            .iter()
            .next()
            .expect("Should have computed one command");
        assert_eq!(
            command,
            &Command::StartActor(StartActor {
                reference: actor_reference,
                host_id,
                count: replicas
            })
        )
    }

    #[tokio::test]
    async fn can_request_multiple_stop_actor() {
        let lattice_id = "hoohah_multi_stop_actor";
        let actor_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let actor_id = "MASDASDIAMAREALACTOR";
        let replicas = 2;

        let store = Arc::new(TestStore::default());
        let worker = EventWorker::new(store.clone(), HashMap::default());

        // *** STATE SETUP BEGIN ***
        // Lattice State: One host with 4 instances of the actor, and one host with 3 instances

        let host_id_1 = "NASDASDIAMAREALHOST".to_string();
        let host_name_1 = "I am a real host".to_string();
        let host_id_2 = "NASDASDIAMAREALHOSTV2FINAL".to_string();
        let host_name_2 = "I am a real host v2 final".to_string();

        let labels = HashMap::from([("real".to_string(), "true".to_string())]);

        worker
            .do_work(ScopedMessage::<Event> {
                lattice_id: lattice_id.to_string(),
                inner: Event::HostStarted(HostStarted {
                    labels: labels.clone(),
                    friendly_name: host_name_1,
                    id: host_id_1.to_string(),
                }),
                acker: None,
            })
            .await
            .expect("Should be able to handle the host started event");

        worker
            .do_work(ScopedMessage::<Event> {
                lattice_id: lattice_id.to_string(),
                inner: Event::HostStarted(HostStarted {
                    labels,
                    friendly_name: host_name_2,
                    id: host_id_2.to_string(),
                }),
                acker: None,
            })
            .await
            .expect("Should be able to handle the host started event");

        for n in 0..4 {
            // Start 4 actors on the first host
            worker
                .do_work(ScopedMessage::<Event> {
                    lattice_id: lattice_id.to_string(),
                    inner: Event::ActorStarted(ActorStarted {
                        annotations: HashMap::new(),
                        api_version: 1, // ??
                        claims: dummy_actor_claims(),
                        image_ref: actor_reference.to_string(),
                        instance_id: format!("{actor_id}_{host_id_1}_{n}"),
                        public_key: actor_id.to_string(),
                        host_id: host_id_1.to_string(),
                    }),
                    acker: None,
                })
                .await
                .expect("Should be able to handle the actor started event");
        }

        for n in 0..3 {
            // Start 3 actors on the second host
            worker
                .do_work(ScopedMessage::<Event> {
                    lattice_id: lattice_id.to_string(),
                    inner: Event::ActorStarted(ActorStarted {
                        annotations: HashMap::new(),
                        api_version: 1, // ??
                        claims: dummy_actor_claims(),
                        image_ref: actor_reference.to_string(),
                        instance_id: format!("{actor_id}_{host_id_2}_{n}"),
                        public_key: actor_id.to_string(),
                        host_id: host_id_2.to_string(),
                    }),
                    acker: None,
                })
                .await
                .expect("Should be able to handle the actor started event");
        }
        // *** STATE SETUP END ***
        // Expected Actions: Two ActorStop commands, requesting either (depending on map order):
        // - 4 stopped actors on the first host and 1 on the other
        // - 2 stopped actors on the first host and 3 on the other

        let simple_scaler = SimpleActorScaler::new(
            store.clone(),
            actor_reference.to_string(),
            lattice_id.to_string(),
            replicas,
        );

        let cmds = simple_scaler
            .reconcile()
            .await
            .expect("Should have computed a set of commands");

        assert_eq!(cmds.len(), 2);

        // Asserting we requested 5 total stops, whether 4 and 1 or 3 and 2
        let stop_count_requested = cmds
            .iter()
            .map(|cmd| match cmd {
                Command::StopActor(stop_cmd) => stop_cmd.count,
                _ => panic!("unexpected command in list"),
            })
            .sum::<usize>();

        assert_eq!(stop_count_requested, 5)
    }

    // helper function to Returns dummy actor claims object
    fn dummy_actor_claims() -> ActorClaims {
        ActorClaims {
            call_alias: None,
            capabilites: vec![],
            expires_human: "N/A".to_string(),
            issuer: "AASDASDIAMAREALISSUER".to_string(),
            name: "real actor".to_string(),
            not_before_human: "N/A".to_string(),
            revision: 1,
            tags: None,
            version: "v0.1.0".to_string(),
        }
    }
}
