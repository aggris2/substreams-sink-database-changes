use crate::pb::database::{table_change::Operation, DatabaseChanges, Field, TableChange};
use std::collections::{BTreeMap, HashMap};
use substreams::{
    scalar::{BigDecimal, BigInt},
    Hex,
};

#[derive(Debug)]
pub struct Tables {
    // Map from table name to the primary keys within that table
    pub tables: HashMap<String, Rows>,

    // Ordinal is used to track the order of changes, it is incremented for each row
    // in such way that at the end, we can correctly order the changes back correctly.
    ordinal: Ordinal,
}

impl Tables {
    pub fn new() -> Self {
        Tables {
            tables: HashMap::new(),
            ordinal: Ordinal::new(),
        }
    }

    /// Returns the number of rows in all tables.
    pub fn all_row_count(&self) -> usize {
        self.tables.values().map(|rows| rows.pks.len()).sum()
    }

    /// Create a new row in the table with the given primary key.
    ///
    /// ```
    /// // With a Primary Key of type `Single`
    /// use crate::substreams_database_change::tables::Tables;
    /// let mut tables = Tables::new();
    /// tables.create_row("myevent", "my_key",);
    /// ```
    ///
    /// ```
    /// // With a Primary Key of type `Composite`
    /// use crate::substreams_database_change::tables::Tables;
    /// let mut tables = Tables::new();
    /// tables.create_row("myevent", [("evt_tx_hash", String::from("hello")), ("evt_index", String::from("world"))]);
    /// ```
    pub fn create_row<K: Into<PrimaryKey>>(&mut self, table: &str, key: K) -> &mut Row {
        let rows: &mut Rows = self.tables.entry(table.to_string()).or_insert(Rows::new());
        let k = key.into();
        let key_debug = format!("{:?}", k);
        let row = rows
            .pks
            .entry(k)
            .or_insert(Row::new_ordered(self.ordinal.next()));
        match row.operation {
            Operation::Unspecified => {
                row.operation = Operation::Create;
            }
            Operation::Create => { /* Already the right operation */ }
            Operation::Upsert => {
                panic!(
                    "cannot create a row after a scheduled upsert operation, create and upsert are exclusive - table: {} key: {}",
                    table, key_debug,
                )
            }
            Operation::Update => {
                panic!("cannot create a row that was marked for update")
            }
            Operation::Delete => {
                panic!(
                    "cannot create a row after a scheduled delete operation - table: {} key: {}",
                    table, key_debug,
                )
            }
        }
        row
    }

    /// Upsert (insert or update) a new row in the table with the given primary key.
    ///
    /// *Note* Ensure that the SQL sink driver you use supports upsert operations.
    ///
    /// ```
    /// // With a Primary Key of type `Single`
    /// use crate::substreams_database_change::tables::Tables;
    /// let mut tables = Tables::new();
    /// tables.upsert_row("myevent", "my_key",);
    /// ```
    ///
    /// ```
    /// // With a Primary Key of type `Composite`
    /// use crate::substreams_database_change::tables::Tables;
    /// let mut tables = Tables::new();
    /// tables.upsert_row("myevent", [("evt_tx_hash", String::from("hello")), ("evt_index", String::from("world"))]);
    /// ```
    pub fn upsert_row<K: Into<PrimaryKey>>(&mut self, table: &str, key: K) -> &mut Row {
        let rows = self.tables.entry(table.to_string()).or_insert(Rows::new());
        let k = key.into();
        let key_debug = format!("{:?}", k);
        let row = rows
            .pks
            .entry(k)
            .or_insert(Row::new_ordered(self.ordinal.next()));
        match row.operation {
            Operation::Unspecified => {
                row.operation = Operation::Upsert;
            }
            Operation::Create => {
                panic!(
                    "cannot upsert a row after a scheduled create operation, create and upsert are exclusive - table: {} key: {}",
                    table, key_debug,
                )
            }
            Operation::Upsert => { /* Already the right operation */ }
            Operation::Update => {
                panic!(
                    "cannot upsert a row after a scheduled update operation, update and upsert are exclusive - table: {} key: {}",
                    table, key_debug,
                )
            }
            Operation::Delete => {
                panic!(
                    "cannot upsert a row after a scheduled delete operation - table: {} key: {}",
                    table, key_debug,
                )
            }
        }
        row
    }

    pub fn update_row<K: Into<PrimaryKey>>(&mut self, table: &str, key: K) -> &mut Row {
        let rows = self.tables.entry(table.to_string()).or_insert(Rows::new());
        let k = key.into();
        let key_debug = format!("{:?}", k);
        let row = rows
            .pks
            .entry(k)
            .or_insert(Row::new_ordered(self.ordinal.next()));
        match row.operation {
            Operation::Unspecified => {
                row.operation = Operation::Update;
            }
            Operation::Create => { /* Fine, updated columns will be part of Insert operation */ }
            Operation::Upsert => { /* Fine, updated columns will be part of Upsert operation */ }
            Operation::Update => { /* Already the right operation */ }
            Operation::Delete => {
                panic!(
                    "cannot update a row after a scheduled delete operation - table: {} key: {}",
                    table, key_debug,
                )
            }
        }
        row
    }

    pub fn delete_row<K: Into<PrimaryKey>>(&mut self, table: &str, key: K) -> &mut Row {
        let rows = self.tables.entry(table.to_string()).or_insert(Rows::new());
        let row = rows
            .pks
            .entry(key.into())
            .or_insert(Row::new_ordered(self.ordinal.next()));

        row.columns = HashMap::new();
        row.operation = match row.operation {
            Operation::Unspecified => Operation::Delete,
            Operation::Create => {
                // We are creating the row in this block, there is no need to emit a DELETE statement,
                // we specify Unspecified and the row will be skipped when comes the time to emit the
                // changes.
                Operation::Unspecified
            }
            Operation::Upsert => {
                // We cannot know if the row was created within that block or already present
                // in the database. As such, we must emit a DELETE statement in the sink
                // for this. Worst case, the DELETE will hit no row and be a no-op.
                Operation::Delete
            }
            Operation::Update => {
                // The row must be deleted, emit the operation
                Operation::Delete
            }
            Operation::Delete => {
                // Already delete type, continue using that as the operation
                Operation::Delete
            }
        };

        row
    }

    // Convert Tables into an DatabaseChanges protobuf object
    pub fn to_database_changes(self) -> DatabaseChanges {
        let mut changes = DatabaseChanges::default();

        for (table, rows) in self.tables.into_iter() {
            for (pk, row) in rows.pks.into_iter() {
                if row.operation == Operation::Unspecified {
                    continue;
                }

                let mut change = match pk {
                    PrimaryKey::Single(pk) => {
                        TableChange::new(table.clone(), pk, row.ordinal, row.operation)
                    }
                    PrimaryKey::Composite(keys) => TableChange::new_composite(
                        table.clone(),
                        keys.into_iter().collect(),
                        row.ordinal,
                        row.operation,
                    ),
                };

                for (field, value) in row.columns.into_iter() {
                    change.fields.push(Field {
                        name: field,
                        new_value: value,
                        old_value: "".to_string(),
                    });
                }

                changes.table_changes.push(change);
            }
        }

        changes.table_changes.sort_by_key(|change| change.ordinal);
        changes
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Ordinal(u64);

impl Ordinal {
    pub fn new() -> Self {
        Ordinal(0)
    }

    pub fn next(&mut self) -> u64 {
        let current = self.0;
        self.0 += 1;
        current
    }
}

#[derive(Hash, Debug, Eq, PartialEq)]
pub enum PrimaryKey {
    Single(String),
    Composite(BTreeMap<String, String>),
}

impl From<&str> for PrimaryKey {
    fn from(x: &str) -> Self {
        Self::Single(x.to_string())
    }
}

impl From<&String> for PrimaryKey {
    fn from(x: &String) -> Self {
        Self::Single(x.clone())
    }
}

impl From<String> for PrimaryKey {
    fn from(x: String) -> Self {
        Self::Single(x)
    }
}

impl<K: AsRef<str>, const N: usize> From<[(K, String); N]> for PrimaryKey {
    fn from(arr: [(K, String); N]) -> Self {
        if N == 0 {
            return Self::Composite(BTreeMap::new());
        }

        let string_arr = arr.map(|(k, v)| (k.as_ref().to_string(), v));
        Self::Composite(BTreeMap::from(string_arr))
    }
}

impl<K: AsRef<str>, const N: usize> From<[(K, &str); N]> for PrimaryKey {
    fn from(arr: [(K, &str); N]) -> Self {
        if N == 0 {
            return Self::Composite(BTreeMap::new());
        }

        let string_arr = arr.map(|(k, v)| (k.as_ref().to_string(), v.to_string()));
        Self::Composite(BTreeMap::from(string_arr))
    }
}

#[derive(Debug)]
pub struct Rows {
    // Map of primary keys within this table, to the fields within
    pks: HashMap<PrimaryKey, Row>,
}

impl Rows {
    pub fn new() -> Self {
        Rows {
            pks: HashMap::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct Row {
    /// Verify that we don't try to delete the same row as we're creating it
    pub operation: Operation,
    /// Map of field name to its last change
    pub columns: HashMap<String, String>,
    /// Finalized: Last update or delete
    #[deprecated(
        note = "The finalization state is now implicitly handled by the `operation` field."
    )]
    pub finalized: bool,

    ordinal: u64,
}

impl Row {
    /// **Do not use** Now broken, use the `Tables` API instead like `create_row`, `upsert_row`, `update_row`, or `delete_row`.
    /// Kept for code compilation but it's expected that this was never used in practice.
    #[deprecated(
        note = "Do now create a new row manually, use the `Tables` API instead like `create_row`, `upsert_row`, `update_row`, or `delete_row`"
    )]
    pub fn new() -> Self {
        Row {
            operation: Operation::Unspecified,
            columns: HashMap::new(),
            ..Default::default()
        }
    }

    pub(crate) fn new_ordered(ordinal: u64) -> Self {
        Row {
            operation: Operation::Unspecified,
            columns: HashMap::new(),
            ordinal,
            ..Default::default()
        }
    }

    /// Set a field to a value, this is the standard method for setting fields in a row.
    ///
    /// This method ensures that the value is converted to a database-compatible format
    /// using the `ToDatabaseValue` trait. It is the primary way to set fields in a row
    /// for most use cases.
    ///
    /// The `ToDatabaseValue` trait is implemented for various types, including primitive
    /// types, strings, and custom types. This allows you to set fields with different
    /// types of values without worrying about the underlying conversion. Check example
    /// for more details.
    ///
    /// Check [ToDatabaseValue] for implemented automatic conversions.
    ///
    /// # Panics
    ///
    /// This method will panic if called on a row marked for deletion.
    ///
    /// # Example
    ///
    /// ```
    /// use substreams::scalar::{BigInt, BigDecimal};
    /// use crate::substreams_database_change::tables::Tables;
    /// let mut tables = Tables::new();
    /// let row = tables.create_row("myevent", "my_key");
    /// row.set("name", "asset name");
    /// row.set("decimals", 42);
    /// row.set("count", BigDecimal::from(42));
    /// row.set("value", BigInt::from(42));
    /// ```
    pub fn set<T: ToDatabaseValue>(&mut self, name: &str, value: T) -> &mut Self {
        if self.operation == Operation::Delete {
            panic!("cannot set fields on a delete operation")
        }
        self.columns.insert(name.to_string(), value.to_value());
        self
    }

    /// Set a field to a raw value, this is useful for setting values that are not
    /// normalized across all databases. In there, you can put the raw value as you
    /// would in a SQL statement of the database you are targeting.
    ///
    /// This will be pass as a string to the database which will interpret it itself.
    pub fn set_raw(&mut self, name: &str, value: String) -> &mut Self {
        self.columns.insert(name.to_string(), value);
        self
    }

    /// Set a field to an array of values compatible with PostgresSQL database,
    /// this method is currently experimental and hidden as we plan to support
    /// array natively in the model.
    ///
    /// For now, this method should be used with great care as it ties the model
    /// to the database implementation.
    #[doc(hidden)]
    pub fn set_psql_array<T: ToDatabaseValue>(&mut self, name: &str, value: Vec<T>) -> &mut Row {
        if self.operation == Operation::Delete {
            panic!("cannot set fields on a delete operation")
        }

        let values = value
            .into_iter()
            .map(|x| x.to_value())
            .collect::<Vec<_>>()
            .join(",");

        self.columns
            .insert(name.to_string(), format!("'{{{}}}'", values));
        self
    }

    /// Set a field to an array of values compatible with Clickhouse database,
    /// this method is currently experimental and hidden as we plan to support
    /// array natively in the model.
    ///
    /// For now, this method should be used with great care as it ties the model
    /// to the database implementation.
    #[doc(hidden)]
    pub fn set_clickhouse_array<T: ToDatabaseValue>(
        &mut self,
        name: &str,
        value: Vec<T>,
    ) -> &mut Row {
        if self.operation == Operation::Delete {
            panic!("cannot set fields on a delete operation")
        }

        let values = value
            .into_iter()
            .map(|x| x.to_value())
            .collect::<Vec<_>>()
            .join(",");

        self.columns
            .insert(name.to_string(), format!("[{}]", values));
        self
    }
}

macro_rules! impl_to_database_value_proxy_to_ref {
    ($name:ty) => {
        impl ToDatabaseValue for $name {
            fn to_value(self) -> String {
                ToDatabaseValue::to_value(&self)
            }
        }
    };
}

macro_rules! impl_to_database_value_proxy_to_string {
    ($name:ty) => {
        impl ToDatabaseValue for $name {
            fn to_value(self) -> String {
                ToString::to_string(&self)
            }
        }
    };
}

pub trait ToDatabaseValue {
    fn to_value(self) -> String;
}

impl_to_database_value_proxy_to_string!(i8);
impl_to_database_value_proxy_to_string!(i16);
impl_to_database_value_proxy_to_string!(i32);
impl_to_database_value_proxy_to_string!(i64);
impl_to_database_value_proxy_to_string!(u8);
impl_to_database_value_proxy_to_string!(u16);
impl_to_database_value_proxy_to_string!(u32);
impl_to_database_value_proxy_to_string!(u64);
impl_to_database_value_proxy_to_string!(bool);
impl_to_database_value_proxy_to_string!(::prost_types::Timestamp);
impl_to_database_value_proxy_to_string!(&::prost_types::Timestamp);
impl_to_database_value_proxy_to_string!(&str);
impl_to_database_value_proxy_to_string!(BigDecimal);
impl_to_database_value_proxy_to_string!(&BigDecimal);
impl_to_database_value_proxy_to_string!(BigInt);
impl_to_database_value_proxy_to_string!(&BigInt);

impl_to_database_value_proxy_to_ref!(Vec<u8>);

impl ToDatabaseValue for &String {
    fn to_value(self) -> String {
        self.clone()
    }
}

impl ToDatabaseValue for String {
    fn to_value(self) -> String {
        self
    }
}

impl ToDatabaseValue for &Vec<u8> {
    fn to_value(self) -> String {
        Hex::encode(self)
    }
}

impl<T: AsRef<[u8]>> ToDatabaseValue for Hex<T> {
    fn to_value(self) -> String {
        ToString::to_string(&self)
    }
}

impl<T: AsRef<[u8]>> ToDatabaseValue for &Hex<T> {
    fn to_value(self) -> String {
        ToString::to_string(self)
    }
}

#[cfg(test)]
mod test {
    use crate::pb::database::table_change::PrimaryKey as PrimaryKeyProto;
    use crate::pb::database::CompositePrimaryKey as CompositePrimaryKeyProto;
    use crate::pb::database::{DatabaseChanges, TableChange};
    use crate::tables::PrimaryKey;
    use crate::tables::Tables;
    use crate::tables::ToDatabaseValue;
    use pretty_assertions::assert_eq;

    #[test]
    fn to_database_value_proto_timestamp() {
        assert_eq!(
            ToDatabaseValue::to_value(::prost_types::Timestamp {
                seconds: 60 * 60 + 60 + 1,
                nanos: 1
            }),
            "1970-01-01T01:01:01.000000001Z"
        );
    }

    #[test]
    fn create_row_single_pk_direct() {
        let mut tables = Tables::new();
        tables.create_row("myevent", PrimaryKey::Single("myhash".to_string()));

        assert_eq!(
            tables.to_database_changes(),
            DatabaseChanges {
                table_changes: [change("myevent", "myhash", 0)].to_vec(),
            }
        );
    }

    #[test]
    fn create_row_single_pk() {
        let mut tables = Tables::new();
        tables.create_row("myevent", "myhash");

        assert_eq!(
            tables.to_database_changes(),
            DatabaseChanges {
                table_changes: [change("myevent", "myhash", 0)].to_vec(),
            }
        );
    }

    #[test]
    fn create_row_composite_pk() {
        let mut tables = Tables::new();
        tables.create_row(
            "myevent",
            [("evt_tx_hash", "hello"), ("evt_index", "world")],
        );

        assert_eq!(
            tables.to_database_changes(),
            DatabaseChanges {
                table_changes: [change(
                    "myevent",
                    [("evt_tx_hash", "hello"), ("evt_index", "world")],
                    0
                )]
                .to_vec()
            }
        );
    }

    #[test]
    fn row_ordering() {
        let mut tables = Tables::new();
        tables.create_row("tableA", "one");
        tables.create_row("tableC", "two");
        tables.create_row("tableA", "three");
        tables.create_row("tableD", "four");
        tables.create_row("tableE", "five");
        tables.create_row("tableC", "six");

        assert_eq!(
            tables.to_database_changes(),
            DatabaseChanges {
                table_changes: [
                    change("tableA", "one", 0),
                    change("tableC", "two", 1),
                    change("tableA", "three", 2),
                    change("tableD", "four", 3),
                    change("tableE", "five", 4),
                    change("tableC", "six", 5)
                ]
                .to_vec(),
            }
        );
    }

    fn change<K: Into<PrimaryKey>>(name: &str, key: K, ordinal: u64) -> TableChange {
        TableChange {
            table: name.to_string(),
            ordinal,
            operation: 1,
            fields: [].into(),
            primary_key: Some(match key.into() {
                PrimaryKey::Single(pk) => PrimaryKeyProto::Pk(pk),
                PrimaryKey::Composite(keys) => {
                    PrimaryKeyProto::CompositePk(CompositePrimaryKeyProto {
                        keys: keys.into_iter().collect(),
                    })
                }
            }),
        }
    }
}
