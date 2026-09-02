//! Tests for `ActivityTypeResolver` and `MapBasedResolver`.

use ascetic_ddd_saga::{
    Activity, ActivityType, ActivityTypeResolver, MapBasedResolver, Result, RoutingSlip, SagaError,
    WorkItem, WorkLog, WorkResult, async_trait,
};

/// Activity that reports an explicit canonical name.
#[derive(Debug, Default)]
struct NamedTestActivity;

#[async_trait]
impl Activity for NamedTestActivity {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        Ok(Some(WorkLog::new(self, WorkResult::new())))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./test"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./testCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }

    fn type_name(&self) -> Option<&str> {
        Some("NamedTestActivity")
    }
}

/// Second named activity, for multi-registration tests.
#[derive(Debug, Default)]
struct AnotherNamedActivity;

#[async_trait]
impl Activity for AnotherNamedActivity {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        Ok(Some(WorkLog::new(self, WorkResult::new())))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./another"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./anotherCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }

    fn type_name(&self) -> Option<&str> {
        Some("AnotherNamedActivity")
    }
}

/// Activity that reports no canonical name.
#[derive(Debug, Default)]
struct AnonymousActivity;

#[async_trait]
impl Activity for AnonymousActivity {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        Ok(Some(WorkLog::new(self, WorkResult::new())))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./anon"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./anonCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

#[test]
fn register_and_resolve() {
    let mut resolver = MapBasedResolver::new();
    resolver.register_type::<NamedTestActivity>("NamedTestActivity");

    let resolved = resolver.resolve("NamedTestActivity").unwrap();

    assert_eq!(resolved, ActivityType::of::<NamedTestActivity>());
    assert_eq!(resolved.create().work_item_queue_address(), "sb://./test");
}

#[test]
fn resolve_unregistered_type_is_reported() {
    let resolver = MapBasedResolver::new();

    assert!(matches!(
        resolver.resolve("UnregisteredActivity"),
        Err(SagaError::ActivityTypeNotRegistered(name)) if name == "UnregisteredActivity",
    ));
}

#[test]
fn multiple_registrations() {
    let mut resolver = MapBasedResolver::new();
    resolver.register_type::<NamedTestActivity>("NamedTestActivity");
    resolver.register_type::<AnotherNamedActivity>("AnotherNamedActivity");

    assert_eq!(
        resolver.resolve("NamedTestActivity").unwrap(),
        ActivityType::of::<NamedTestActivity>(),
    );
    assert_eq!(
        resolver.resolve("AnotherNamedActivity").unwrap(),
        ActivityType::of::<AnotherNamedActivity>(),
    );
}

#[test]
fn register_overwrite() {
    let mut resolver = MapBasedResolver::new();
    resolver.register_type::<NamedTestActivity>("TestActivity");
    resolver.register_type::<AnotherNamedActivity>("TestActivity");

    assert_eq!(
        resolver.resolve("TestActivity").unwrap(),
        ActivityType::of::<AnotherNamedActivity>(),
    );
}

#[test]
fn get_name_for_registered_type() {
    let mut resolver = MapBasedResolver::new();
    resolver.register_type::<NamedTestActivity>("NamedTestActivity");

    assert_eq!(
        resolver
            .get_name(ActivityType::of::<NamedTestActivity>())
            .unwrap(),
        "NamedTestActivity",
    );
}

#[test]
fn get_name_falls_back_to_the_activity_name() {
    let resolver = MapBasedResolver::new();

    // Intentionally not registered.
    assert_eq!(
        resolver
            .get_name(ActivityType::of::<NamedTestActivity>())
            .unwrap(),
        "NamedTestActivity",
    );
}

#[test]
fn get_name_unregistered_anonymous_is_reported() {
    let resolver = MapBasedResolver::new();

    assert!(matches!(
        resolver.get_name(ActivityType::of::<AnonymousActivity>()),
        Err(SagaError::ActivityTypeNotRegistered(name)) if name == "AnonymousActivity",
    ));
}

/// Each resolver is independent -- no shared global state.
#[test]
fn isolated_instances() {
    let mut resolver_a = MapBasedResolver::new();
    let resolver_b = MapBasedResolver::new();
    resolver_a.register_type::<NamedTestActivity>("NamedTestActivity");

    assert_eq!(
        resolver_a.resolve("NamedTestActivity").unwrap(),
        ActivityType::of::<NamedTestActivity>(),
    );
    assert!(resolver_b.resolve("NamedTestActivity").is_err());
}

/// The counterpart of Python's `isinstance(activity, NamedActivity)`.
#[test]
fn named_activity_reports_its_name() {
    assert_eq!(NamedTestActivity.type_name(), Some("NamedTestActivity"));
}

#[test]
fn anonymous_activity_reports_no_name() {
    assert_eq!(AnonymousActivity.type_name(), None);
}
