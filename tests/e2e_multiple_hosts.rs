use std::path::PathBuf;

use wadm::server::{DeployResult, PutResult};
use wadm::APP_SPEC_ANNOTATION;

mod e2e;

use e2e::{assert_status, ClientInfo};

const MANIFESTS_PATH: &str = "test/data";
const DOCKER_COMPOSE_FILE: &str = "test/docker-compose-e2e.yaml";

// NOTE(thomastaylor312): This exists because we need to have setup happen only once for all tests
// and then we want cleanup to run with `Drop`. I tried doing this with a `OnceCell`, but `static`s
// don't run drop, they only drop the memory (I also think OnceCell does the same thing too). So to
// get around this we have a top level test that runs everything
#[cfg(feature = "_e2e_tests")]
#[tokio::test(flavor = "multi_thread")]
async fn run_all_tests() {
    let root_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Unable to find repo root"));
    let manifest_dir = root_dir.join(MANIFESTS_PATH);
    let compose_file = root_dir.join(DOCKER_COMPOSE_FILE);

    let client_info = ClientInfo::new(manifest_dir, compose_file).await;

    // NOTE(thomastaylor312): A nice to have here, but what I didn't want to figure out now, would
    // be to catch the panics from tests and label the backtrace with the appropriate information
    // about which test failed. Another issue is that only the first panic will be returned, so
    // capturing the backtraces and then printing them nicely would probably be good

    let tests = [test_no_requirements(&client_info)];
    futures::future::join_all(tests).await;
}

async fn test_no_requirements(client_info: &ClientInfo) {
    let resp = client_info
        .put_manifest_from_file("simple.yaml", None)
        .await;

    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest"
    );

    let resp = client_info.deploy_manifest("echo-simple", None, None).await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest"
    );

    // NOTE: This runs for a while, but it's because we're waiting for the provider to download,
    // which can take a bit
    assert_status(None, Some(5), || async {
        let inventory = client_info.get_all_inventory().await;

        let all_echo_actors = inventory
            .values()
            .flat_map(|inv| &inv.actors)
            .filter_map(|actor| {
                (actor.image_ref.as_deref().unwrap_or_default()
                    == "wasmcloud.azurecr.io/echo:0.3.7")
                    .then_some(&actor.instances)
            })
            .flatten();
        let actor_count = all_echo_actors
            .filter(|actor| {
                actor
                    .annotations
                    .as_ref()
                    .and_then(|annotations| {
                        annotations
                            .get(APP_SPEC_ANNOTATION)
                            .map(|val| val == "echo-simple")
                    })
                    .unwrap_or(false)
            })
            .count();
        if actor_count != 4 {
            anyhow::bail!(
                "Should have had 4 actors managed by wadm running, only found {actor_count}"
            )
        }

        // Get count of providers matching annotation
        let provider_count = inventory
            .values()
            .flat_map(|inv| &inv.providers)
            .filter(|provider| {
                provider.image_ref.as_deref().unwrap_or_default()
                    == "wasmcloud.azurecr.io/httpserver:0.17.0"
                    && provider
                        .annotations
                        .as_ref()
                        .and_then(|annotations| {
                            annotations
                                .get(APP_SPEC_ANNOTATION)
                                .map(|val| val == "echo-simple")
                        })
                        .unwrap_or(false)
            })
            .count();

        if provider_count != 1 {
            anyhow::bail!(
                "Should have had 1 provider managed by wadm running, found {provider_count}"
            )
        }
        Ok(())
    })
    .await;
}
