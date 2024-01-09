use super::query_emission::QuerySender;

pub trait QueryHandler {
    type Q: Send + Sync + std::fmt::Debug; // Query type
    type R: Send + Sync + std::fmt::Debug; // Query response type

    fn connect(&self) -> QuerySender<Self::Q, Self::R>;
}
