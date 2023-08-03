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
    /// (e.g. `A****.wasmbus.evt.{lattice-id}`) if it doesn't match the normally expected pattern
    pub fn new(prefix: &str, multitenant: bool) -> LatticeIdParser {
        LatticeIdParser {
            prefix: prefix.to_owned(),
            multitenant,
        }
    }

    /// Parses the given subject based on settings and then returns the lattice ID of the subject and
    /// the account ID if it is multitenant.
    /// Returns None if it couldn't parse the topic
    pub fn parse<'a>(&self, subject: &'a str) -> (Option<&'a str>, Option<&'a str>) {
        let separated: Vec<&str> = subject.split('.').collect();
        // For reference, topics look like the following:
        //
        // Normal: `{prefix}.evt.{lattice-id}`
        // Multitenant: `{account-id}.{prefix}.evt.{lattice-id}`
        //
        // Note that the account ID should be prefaced with an `A`
        match separated[..] {
            [prefix, evt, lattice_id] if prefix == self.prefix && evt == EVENT_SUBJECT => {
                (Some(lattice_id), None)
            }
            [account_id, prefix, evt, lattice_id]
                if self.multitenant
                    && prefix == self.prefix
                    && evt == EVENT_SUBJECT
                    && account_id.starts_with('A') =>
            {
                (Some(lattice_id), Some(account_id))
            }
            _ => (None, None),
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

        assert_eq!(
            parser
                .parse("wasmbus.evt.blahblah")
                .0
                .expect("Should return lattice id"),
            "blahblah",
            "Should return the right ID"
        );

        // Shouldn't parse a multitenant
        assert!(
            parser.parse("ACCOUNTID.wasmbus.evt.default").0.is_none(),
            "Shouldn't parse a multitenant topic"
        );

        // Multitenant second
        let parser = LatticeIdParser::new("wasmbus", true);

        assert_eq!(
            parser
                .parse("wasmbus.evt.blahblah")
                .0
                .expect("Should return lattice id"),
            "blahblah",
            "Should return the right ID"
        );

        let res = parser.parse("ACCOUNTID.wasmbus.evt.blahblah");

        assert_eq!(
            res.0.expect("Should return lattice id"),
            "blahblah",
            "Should return the right ID"
        );

        assert_eq!(
            res.1.expect("Should return account id in multitenant mode"),
            "ACCOUNTID",
            "Should return the right ID"
        );
    }

    #[test]
    fn test_invalid_subjects() {
        let parser = LatticeIdParser::new("wasmbus", true);

        // Test 3 and 4 part subjects to make sure they don't parse
        assert!(
            parser.parse("BLAH.wasmbus.notevt.default").0.is_none(),
            "Shouldn't parse 4 part invalid topic"
        );

        assert!(
            parser.parse("wasmbus.notme.default").0.is_none(),
            "Shouldn't parse 3 part invalid topic"
        );

        assert!(
            parser.parse("lebus.evt.default").0.is_none(),
            "Shouldn't parse an non-matching prefix"
        );

        assert!(
            parser.parse("wasmbus.evt").0.is_none(),
            "Shouldn't parse a too short topic"
        );

        assert!(
            parser.parse("BADACCOUNT.wasmbus.evt.default").0.is_none(),
            "Shouldn't parse invalid account topic"
        );

        assert!(
            parser.parse("wasmbus.notme.default.bar.baz").0.is_none(),
            "Shouldn't parse long topic"
        );
    }
}
