use crate::ast::{Cursor, SortOptions};

pub const DEFAULT_BATCH_SIZE: u32 = 1000;

impl Cursor {
    pub fn new(start_key: Option<String>, batch_size: Option<u32>) -> Self {
        Self {
            start_key,
            batch_size,
            sort: None,
        }
    }

    pub fn from_previous<T>(
        prev: Option<Cursor>,
        last_key: Option<String>,
        iterable: &[T],
    ) -> Option<Self> {
        if let (Some(prev), Some(last_key)) = (prev.as_ref(), last_key) {
            let batch_size = prev.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
            if iterable.len() as u32 >= batch_size {
                Some(
                    Self::new(Some(last_key), Some(batch_size))
                        .with_sort(prev.sort.clone())
                        .clone(),
                )
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn with_sort(&mut self, sort: Option<SortOptions>) -> &Self {
        self.sort = sort;
        self
    }

    pub fn convert_to_page_params(cursor: Option<&Cursor>) -> (Option<String>, Option<usize>) {
        let start_key = cursor.and_then(|c| c.start_key.clone());
        let limit = cursor.and_then(|c| c.batch_size.map(|b| b as usize));
        (start_key, limit)
    }
}
