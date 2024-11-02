use arrow_schema::Schema;

pub(crate) trait ToSchema {
    fn to_schema() -> Schema;
}
