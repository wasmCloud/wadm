use anyhow::{Context as _, Result};

use wadm_types::validation::{validate_manifest_file, ValidationFailureLevel, ValidationOutput};

/// Ensure that valid YAML manifests are valid
#[tokio::test]
async fn validate_pass() -> Result<()> {
    assert!(
        validate_manifest_file("./tests/fixtures/manifests/simple.wadm.yaml")
            .await?
            .1
            .valid()
    );
    Ok(())
}

/// Ensure that we can detect dangling links
#[tokio::test]
async fn validate_dangling_links() -> Result<()> {
    let (_manifest, failures) =
        validate_manifest_file("./tests/fixtures/manifests/dangling-link.wadm.yaml")
            .await
            .context("failed to validate manifest")?;
    assert!(
        !failures.is_empty()
            && failures
                .iter()
                .all(|f| f.level == ValidationFailureLevel::Warning
                    || f.level == ValidationFailureLevel::Error),
        "failures present, all warnings or errors"
    );
    assert!(!failures.valid(), "manifest should not be valid");
    Ok(())
}

/// Ensure that we can detect misnamed interfaces
#[tokio::test]
async fn validate_misnamed_interface() -> Result<()> {
    let (_manifest, failures) =
        validate_manifest_file("./tests/fixtures/manifests/misnamed-interface.wadm.yaml")
            .await
            .context("failed to validate manifest")?;
    assert!(
        !failures.is_empty()
            && failures
                .iter()
                .all(|f| f.level == ValidationFailureLevel::Warning),
        "failures present, all warnings"
    );
    assert!(
        failures.valid(),
        "manifest should be valid (misnamed interface w/ right namespace & package is probably a bug but might be intentional)"
    );
    Ok(())
}

/// Ensure that we can detect unknown packages under known namespaces
#[tokio::test]
async fn validate_unknown_package() -> Result<()> {
    let (_manifest, failures) =
        validate_manifest_file("./tests/fixtures/manifests/unknown-package.wadm.yaml")
            .await
            .context("failed to validate manifest")?;
    assert!(
        !failures.is_empty()
            && failures
                .iter()
                .all(|f| f.level == ValidationFailureLevel::Warning),
        "failures present, all errors"
    );
    assert!(
        failures.valid(),
        "manifest should be valid (unknown package under a known interface is a warning)"
    );
    Ok(())
}

/// Ensure that we allow through custom interface
#[tokio::test]
async fn validate_custom_interface() -> Result<()> {
    let (_manifest, failures) =
        validate_manifest_file("./tests/fixtures/manifests/custom-interface.wadm.yaml")
            .await
            .context("failed to validate manifest")?;
    assert!(failures.is_empty(), "no failures");
    assert!(
        failures.valid(),
        "manifest is valid (custom namespace is default-allowed)"
    );
    Ok(())
}

#[tokio::test]
async fn validate_bad_manifest() -> Result<()> {
    let result = validate_manifest_file("./tests/fixtures/manifests/made-up-block.wadm.yaml")
        .await
        .context("failed to validate manifest");
    assert!(result.is_err(), "expected error");
    Ok(())
}

#[tokio::test]
async fn validate_bad_manifest_key() -> Result<()> {
    let result = validate_manifest_file("./tests/fixtures/manifests/made-up-key.wadm.yaml")
        .await
        .context("failed to validate manifest");
    assert!(result.is_err(), "expected error");
    Ok(())
}

#[tokio::test]
async fn validate_policy() -> Result<()> {
    let (_manifest, failures) =
        validate_manifest_file("./tests/fixtures/manifests/policy.wadm.yaml")
            .await
            .context("failed to validate manifest")?;
    assert!(failures.is_empty(), "no failures");
    assert!(failures.valid(), "manifest is valid");
    Ok(())
}

/// Ensure that we can detect duplicated link config names
#[tokio::test]
async fn validate_link_config_names() -> Result<()> {
    let (_manifest, failures) =
        validate_manifest_file("./tests/fixtures/manifests/duplicate_link_config_names.wadm.yaml")
            .await
            .context("failed to validate manifest")?;
    let expected_errors = 3;
    assert!(
            !failures.is_empty()
                && failures
                    .iter()
                    .all(|f| f.level == ValidationFailureLevel::Error)
                && failures.len() == expected_errors,
            "expected {} errors because manifest contains {} duplicated link config names, instead {} errors were found", expected_errors, expected_errors, failures.len()
        );
    assert!(
        !failures.valid(),
        "manifest should be invalid (duplicated link config names lead to a dead loop)"
    );
    Ok(())
}

#[tokio::test]
async fn validate_deprecated_configs_raw_yaml() -> Result<()> {
    let (_manifest, failures) = validate_manifest_file(
        "./tests/fixtures/manifests/deprecated-source-and-target-config.yaml",
    )
    .await
    .context("failed to validate manifest")?;
    assert!(failures.valid(), "expected valid manifest");
    assert_eq!(
        failures.warnings().len(),
        2,
        "expected 2 warnings during validating manifest"
    );
    Ok(())
}
