use url::Url;

pub mod body;
pub mod pagination;
pub mod query;

trait UrlExt<Q> {
    fn with_query_params(self, params: Q) -> Self;
}

trait Query {
    fn query_pairs(&self) -> impl Iterator<Item = (&str, &str)>;
}

impl<Q: Query> UrlExt<Q> for Url {
    fn with_query_params(mut self, params: Q) -> Self {
        {
            let mut query_pairs = self.query_pairs_mut();
            for (key, value) in params.query_pairs() {
                query_pairs.append_pair(key, value);
            }
        }
        self
    }
}
