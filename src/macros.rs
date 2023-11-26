macro_rules! ok_or_return {
    ($result:expr, $fallback:expr) => {
        match $result {
            Ok(t) => t,
            Err(err) => {
                #[allow(clippy::redundant_closure_call)]
                $fallback(err);
                return;
            }
        }
    };
}
