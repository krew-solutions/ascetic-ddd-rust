//! Work result - dictionary of results from activity execution.

use crate::value::dict_newtype;

dict_newtype! {
    /// Dictionary containing results from an activity's work execution.
    ///
    /// Stores key-value pairs representing the outcome of
    /// [`do_work()`][crate::activity::Activity::do_work], such as reservation
    /// IDs, confirmation numbers, etc.
    ///
    /// ```
    /// use ascetic_ddd_saga::WorkResult;
    ///
    /// let result = WorkResult::from([("reservationId", 12345)]);
    ///
    /// assert_eq!(result.get_i64("reservationId").unwrap(), 12345);
    /// ```
    WorkResult
}
