mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use support::server;
use tower::{Layer, Service};
use wreq::Client;

/// Counts the connections the client makes.
#[derive(Clone)]
struct CountLayer(Arc<AtomicUsize>);

#[derive(Clone)]
struct CountService<S> {
    inner: S,
    count: Arc<AtomicUsize>,
}

impl<S> Layer<S> for CountLayer {
    type Service = CountService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CountService {
            inner,
            count: self.0.clone(),
        }
    }
}

impl<S: Service<R>, R> Service<R> for CountService<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.call(req)
    }
}

fn counting_client() -> (Client, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let client = Client::builder()
        .connector_layer(CountLayer(count.clone()))
        .no_proxy()
        .build()
        .unwrap();
    (client, count)
}

#[tokio::test]
async fn request_reuses_preconnected_connection() {
    let server = server::http(move |_req| async { http::Response::default() });
    let (client, count) = counting_client();

    client
        .preconnect(format!("http://{}/ignored/path?q=1", server.addr()))
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);

    let res = client
        .get(format!("http://{}/path", server.addr()))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    // the request went out on the preconnected connection
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn preconnect_to_another_origin_is_not_used() {
    let server = server::http(move |_req| async { http::Response::default() });
    let other = server::http(move |_req| async { http::Response::default() });
    let (client, count) = counting_client();

    client
        .preconnect(format!("http://{}", other.addr()))
        .await
        .unwrap();
    client
        .get(format!("http://{}", server.addr()))
        .send()
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn preconnect_errors_like_a_request() {
    let client = Client::builder().no_proxy().build().unwrap();

    // nothing listens on port 1
    let err = client.preconnect("http://127.0.0.1:1").await.unwrap_err();
    assert!(err.is_connect(), "{err:?}");

    let err = client.preconnect("not a url").await.unwrap_err();
    assert!(err.is_builder(), "{err:?}");
}

#[tokio::test]
async fn request_reuses_preconnected_proxy_connection() {
    // a plain http proxy: requests arrive in absolute form
    let proxy = server::http(move |req| async move {
        assert_eq!(req.uri(), "http://hyper.rs.local/prox");
        http::Response::default()
    });
    let count = Arc::new(AtomicUsize::new(0));
    let client = Client::builder()
        .connector_layer(CountLayer(count.clone()))
        .proxy(wreq::Proxy::http(format!("http://{}", proxy.addr())).unwrap())
        .build()
        .unwrap();

    client.preconnect("http://hyper.rs.local").await.unwrap();
    let res = client
        .get("http://hyper.rs.local/prox")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
