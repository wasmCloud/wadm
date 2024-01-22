//! Helper utilities for interacting with NATS
const EVENT_SUBJECT: &str = "evt";

/// A parser for NATS subjects that parses out a lattice ID for any given subject
pub struct LatticeIdParser {
    // NOTE(thomastaylor312): We don't actually support specific prefixes right now, but we could in
    // the future as we already do for control topics. So this is just trying to future proof
    prefix: String,
    multitenant: bool,
}

impl LatticeIdParser {
    /// Returns a new parser configured to use the given prefix. If `multitenant` is set to true,
    /// this parser will also attempt to parse a subject as if it were an account imported topic
    /// (e.g. `A****.wasmbus.evt.{lattice-id}.>`) if it doesn't match the normally expected pattern
    pub fn new(prefix: &str, multitenant: bool) -> LatticeIdParser {
        LatticeIdParser {
            prefix: prefix.to_owned(),
            multitenant,
        }
    }

    /// Parses the given subject based on settings and then returns the lattice ID of the subject and
    /// the account ID if it is multitenant.
    /// Returns None if it couldn't parse the topic
    pub fn parse(&self, subject: &str) -> Option<LatticeInformation> {
        let separated: Vec<&str> = subject.split('.').collect();
        // For reference, topics look like the following:
        //
        // Normal: `{prefix}.evt.{lattice-id}.{event-type}`
        // Multitenant: `{account-id}.{prefix}.evt.{lattice-id}.{event-type}`
        //
        // Note that the account ID should be prefaced with an `A`
        match separated[..] {
            [prefix, evt, lattice_id, _event_type]
                if prefix == self.prefix && evt == EVENT_SUBJECT =>
            {
                Some(LatticeInformation {
                    lattice_id: lattice_id.to_owned(),
                    multitenant_prefix: None,
                    prefix: self.prefix.clone(),
                })
            }
            [account_id, prefix, evt, lattice_id, _event_type]
                if self.multitenant
                    && prefix == self.prefix
                    && evt == EVENT_SUBJECT
                    && account_id.starts_with('A') =>
            {
                Some(LatticeInformation {
                    lattice_id: lattice_id.to_owned(),
                    multitenant_prefix: Some(account_id.to_owned()),
                    prefix: self.prefix.clone(),
                })
            }
            _ => None,
        }
    }
}

/// Simple helper struct for returning lattice information from a parsed event topic
#[derive(Clone, Debug)]
pub struct LatticeInformation {
    lattice_id: String,
    multitenant_prefix: Option<String>,
    prefix: String,
}

impl LatticeInformation {
    pub fn lattice_id(&self) -> &str {
        &self.lattice_id
    }

    pub fn multitenant_prefix(&self) -> Option<&str> {
        self.multitenant_prefix.as_deref()
    }

    /// Constructs the event subject to listen on for a particular lattice
    pub fn event_subject(&self) -> String {
        if let Some(account_id) = &self.multitenant_prefix {
            // e.g. Axxx.wasmbus.evt.{lattice-id}.*
            format!(
                "{}.{}.{}.{}.*",
                account_id, self.prefix, EVENT_SUBJECT, self.lattice_id
            )
        } else {
            // e.g. wasmbus.evt.{lattice-id}.*
            format!("{}.{}.{}.*", self.prefix, EVENT_SUBJECT, self.lattice_id)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_valid_subjects() {
        // Default first
        let parser = LatticeIdParser::new("wasmbus", false);

        let single_lattice = parser
            .parse("wasmbus.evt.blahblah.>")
            .expect("Should return lattice id");
        assert_eq!(
            single_lattice.lattice_id(),
            "blahblah",
            "Should return the right ID"
        );
        assert_eq!(
            single_lattice.multitenant_prefix(),
            None,
            "Should return no multitenant prefix"
        );
        assert_eq!(
            single_lattice.event_subject(),
            "wasmbus.evt.blahblah.*",
            "Should return the right event subject"
        );

        // Shouldn't parse a multitenant
        assert!(
            parser.parse("ACCOUNTID.wasmbus.evt.default.>").is_none(),
            "Shouldn't parse a multitenant topic"
        );

        // Multitenant second
        let parser = LatticeIdParser::new("wasmbus", true);

        assert_eq!(
            parser
                .parse("wasmbus.evt.blahblah.host_heartbeat")
                .expect("Should return lattice id")
                .lattice_id(),
            "blahblah",
            "Should return the right ID"
        );

        let res = parser
            .parse("ACCOUNTID.wasmbus.evt.blahblah.>")
            .expect("Should parse multitenant topic");

        assert_eq!(res.lattice_id(), "blahblah", "Should return the right ID");
        assert_eq!(
            res.multitenant_prefix()
                .expect("Should return account id in multitenant mode"),
            "ACCOUNTID",
            "Should return the right ID"
        );
        assert_eq!(
            res.event_subject(),
            "ACCOUNTID.wasmbus.evt.blahblah.*",
            "Should return the right event subject"
        );
    }

    #[test]
    fn test_invalid_subjects() {
        let parser = LatticeIdParser::new("wasmbus", true);

        // Test 3 and 4 part subjects to make sure they don't parse
        assert!(
            parser.parse("BLAH.wasmbus.notevt.default.>").is_none(),
            "Shouldn't parse 4 part invalid topic"
        );

        assert!(
            parser.parse("wasmbus.notme.default.>").is_none(),
            "Shouldn't parse 3 part invalid topic"
        );

        assert!(
            parser.parse("lebus.evt.default.>").is_none(),
            "Shouldn't parse an non-matching prefix"
        );

        assert!(
            parser.parse("wasmbus.evt.>").is_none(),
            "Shouldn't parse a too short topic"
        );

        assert!(
            parser.parse("BADACCOUNT.wasmbus.evt.default.>").is_none(),
            "Shouldn't parse invalid account topic"
        );

        assert!(
            parser.parse("wasmbus.notme.default.bar.baz").is_none(),
            "Shouldn't parse long topic"
        );
    }
}
