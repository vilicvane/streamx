use std::time::Duration;

use futures::{FutureExt, future::BoxFuture};

pub enum Scheduler {
  Duration(Duration),
  AsyncFn(Box<dyn Fn() -> BoxFuture<'static, ()>>),
}

impl Scheduler {
  pub fn schedule(&self) -> BoxFuture<'static, ()> {
    match self {
      Scheduler::Duration(duration) => tokio::time::sleep(*duration).boxed(),
      Scheduler::AsyncFn(function) => function().boxed(),
    }
  }
}

impl From<Duration> for Scheduler {
  fn from(duration: Duration) -> Self {
    Scheduler::Duration(duration)
  }
}

impl<TFunction, TFuture> From<TFunction> for Scheduler
where
  TFunction: Fn() -> TFuture + 'static,
  TFuture: Future<Output = ()> + Send + 'static,
{
  fn from(function: TFunction) -> Self {
    Scheduler::AsyncFn(Box::new(move || function().boxed()))
  }
}
