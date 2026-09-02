//! Work item arguments - dictionary of input arguments for an activity.

use crate::value::dict_newtype;

dict_newtype! {
    /// Dictionary containing input arguments for an activity.
    ///
    /// Stores key-value pairs representing the parameters needed by an activity
    /// to perform its work, such as vehicle type, room type, destination, etc.
    ///
    /// ```
    /// use ascetic_ddd_saga::{Value, WorkItemArguments};
    ///
    /// let mut arguments = WorkItemArguments::from([("vehicleType", "Compact")]);
    /// arguments.insert("days", 5);
    ///
    /// assert_eq!(arguments.get_str("vehicleType").unwrap(), "Compact");
    /// assert_eq!(arguments.get_i64("days").unwrap(), 5);
    /// assert_eq!(arguments.get("missing"), None::<&Value>);
    /// ```
    WorkItemArguments
}
