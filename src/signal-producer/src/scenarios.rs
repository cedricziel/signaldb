//! # Scenarios
//!
//! Each scenario emits one realistic distributed **trace** that crosses many
//! services in an estate, plus the **logs** and **metrics** those services
//! would produce while serving it. Spans in a trace share a `trace_id` because
//! child spans are created from the parent's [`Context`] even though they belong
//! to different OTLP resources — exactly how a real distributed trace looks.
//!
//! Timings are simulated (see [`Clock`]): every span is laid out on a
//! scenario-local timeline with realistic latencies, and a per-trace `scale`
//! factor jitters the whole trace faster or slower so latency histograms have
//! spread. Roughly one trace in eight is made to fail, injecting error spans,
//! `error`-severity logs and `error.type` metric dimensions.

use opentelemetry::logs::Severity;
use opentelemetry::trace::{SpanKind, TraceContextExt};
use opentelemetry::{Context, KeyValue};
use rand::RngExt;
use rand::rngs::ThreadRng;

use crate::emit::{self, Clock};
use crate::fleet::{Fleet, ServiceTelemetry};

/// The largest timeline offset (in unscaled milliseconds) at which any
/// scenario closes a span — `rideshare_request_ride`'s root closes at 640 ms.
/// [`run`] anchors the scenario clock at least this far (scaled) in the past
/// so even the slowest trace has fully completed by the time it is emitted;
/// the `spans_never_end_in_the_future` test guards this bound.
const MAX_TIMELINE_MS: f64 = 640.0;

/// Per-invocation simulation state passed down through a scenario.
pub struct Sim<'a> {
    fleet: &'a Fleet,
    rng: &'a mut ThreadRng,
    clock: Clock,
    /// Multiplies every timeline offset so traces vary in overall latency.
    scale: f64,
}

impl<'a> Sim<'a> {
    fn svc(&self, namespace: &str, name: &str) -> &'a ServiceTelemetry {
        self.fleet.svc(&format!("{namespace}/{name}"))
    }

    /// Wall-clock time for a timeline offset in milliseconds (after scaling).
    fn t(&self, offset_ms: f64) -> std::time::SystemTime {
        self.clock.at(offset_ms * self.scale)
    }

    /// Seconds between two timeline offsets (after scaling) — for latency metrics.
    fn secs(&self, from_ms: f64, to_ms: f64) -> f64 {
        (to_ms - from_ms) * self.scale / 1_000.0
    }

    fn chance(&mut self, p: f64) -> bool {
        self.rng.random_range(0.0..1.0) < p
    }

    fn pick<'x, T>(&mut self, xs: &'x [T]) -> &'x T {
        &xs[self.rng.random_range(0..xs.len())]
    }
}

/// Runs one scenario for `namespace`, chosen at random with a realistic mix
/// (checkouts/rides are rarer than browsing/pings).
pub fn run(fleet: &Fleet, rng: &mut ThreadRng, namespace: &str) {
    let scale = rng.random_range(0.55..1.9);
    // Age the clock past the whole scaled timeline so every span — including
    // the root, which closes last — ends in the past rather than the future.
    // The extra jitter keeps traces looking like they happened a moment ago
    // at slightly varying ages.
    let age_ms = MAX_TIMELINE_MS * scale + rng.random_range(20.0..180.0);
    let mut sim = Sim {
        fleet,
        rng,
        clock: Clock::starting(age_ms),
        scale,
    };

    match namespace {
        "rideshare" => {
            if sim.chance(0.4) {
                rideshare_request_ride(&mut sim);
            } else {
                rideshare_driver_ping(&mut sim);
            }
        }
        "shop" => {
            if sim.chance(0.35) {
                shop_checkout(&mut sim);
            } else {
                shop_browse(&mut sim);
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Rideshare
// ---------------------------------------------------------------------------

/// A rider requests a trip: handset → edge gateway → trip-service, which fans
/// out to matching (Redis geo + location-service/PostgreSQL) and pricing
/// (Redis surge cache), persists the trip, publishes a Kafka `trip.requested`
/// event, and the notification-service consumes it to push to the driver.
fn rideshare_request_ride(sim: &mut Sim) {
    const NS: &str = "rideshare";
    let rider = sim.svc(NS, "rider-app");
    let gateway = sim.svc(NS, "edge-gateway");
    let trip = sim.svc(NS, "trip-service");
    let matching = sim.svc(NS, "matching-service");
    let location = sim.svc(NS, "location-service");
    let pricing = sim.svc(NS, "pricing-service");
    let notify = sim.svc(NS, "notification-service");

    let trip_id = format!("trip_{:012x}", sim.rng.random_range(0u64..u64::MAX));
    let rider_id = format!("rider_{:08}", sim.rng.random_range(1u64..900_000));
    let city = *sim.pick(&["nyc", "sf", "chi", "bos", "sea"]);
    let no_drivers = sim.chance(0.06);
    let surge_timeout = sim.chance(0.06);
    let failed = no_drivers || surge_timeout;

    // Client span on the rider handset.
    let root = emit::open(
        rider,
        "POST /v1/trips",
        SpanKind::Client,
        &Context::new(),
        sim.t(0.0),
        vec![
            KeyValue::new("http.request.method", "POST"),
            KeyValue::new("url.full", "https://api.rideshare.com/v1/trips"),
            KeyValue::new("server.address", "api.rideshare.com"),
            KeyValue::new("network.connection.type", *sim.pick(&["wifi", "cell"])),
            KeyValue::new("network.connection.subtype", "lte"),
            KeyValue::new("app.screen", "request_ride"),
            KeyValue::new("enduser.id", rider_id.clone()),
        ],
    );

    gateway
        .instruments
        .http_active_requests
        .add(1, &[KeyValue::new("http.route", "/v1/trips")]);

    // Edge gateway server span.
    let gw = emit::open(
        gateway,
        "POST /v1/trips",
        SpanKind::Server,
        &root,
        sim.t(28.0),
        vec![
            KeyValue::new("http.request.method", "POST"),
            KeyValue::new("http.route", "/v1/trips"),
            KeyValue::new("url.path", "/v1/trips"),
            KeyValue::new("url.scheme", "https"),
            KeyValue::new("server.address", "api.rideshare.com"),
            KeyValue::new("client.address", client_ip(sim)),
            KeyValue::new(
                "user_agent.original",
                "RideshareRider/8.24.1 (iPhone; iOS 18.5)",
            ),
        ],
    );
    emit::log(
        gateway,
        &gw,
        Severity::Info,
        format!("routing trip request for {rider_id}"),
        vec![
            KeyValue::new("event.name", "http.request.received"),
            KeyValue::new("enduser.id", rider_id.clone()),
        ],
    );

    // Auth check (internal).
    let auth = emit::open(
        gateway,
        "authorize",
        SpanKind::Internal,
        &gw,
        sim.t(36.0),
        vec![KeyValue::new("auth.method", "jwt")],
    );
    emit::close(&auth, sim.t(64.0));

    // gRPC gateway -> trip-service.
    let trip_rpc = grpc_client(
        sim,
        gateway,
        &gw,
        "rideshare.trip.v1.TripService",
        "CreateTrip",
        70.0,
    );
    let trip_srv = grpc_server(
        sim,
        trip,
        &trip_rpc,
        "rideshare.trip.v1.TripService",
        "CreateTrip",
        76.0,
    );
    trip_srv
        .span()
        .set_attribute(KeyValue::new("trip.id", trip_id.clone()));
    trip_srv
        .span()
        .set_attribute(KeyValue::new("geo.city", city));

    // trip-service -> matching-service.
    let match_rpc = grpc_client(
        sim,
        trip,
        &trip_srv,
        "rideshare.matching.v1.MatchingService",
        "FindDrivers",
        88.0,
    );
    let match_srv = grpc_server(
        sim,
        matching,
        &match_rpc,
        "rideshare.matching.v1.MatchingService",
        "FindDrivers",
        94.0,
    );

    // Redis geo search for nearby drivers.
    redis_op(
        sim,
        matching,
        &match_srv,
        "GEOSEARCH",
        "drivers:geo",
        100.0,
        156.0,
    );

    // matching -> location-service -> PostgreSQL.
    let loc_rpc = grpc_client(
        sim,
        matching,
        &match_srv,
        "rideshare.location.v1.LocationService",
        "ResolveCells",
        166.0,
    );
    let loc_srv = grpc_server(
        sim,
        location,
        &loc_rpc,
        "rideshare.location.v1.LocationService",
        "ResolveCells",
        172.0,
    );
    postgres_op(
        sim,
        location,
        &loc_srv,
        "SELECT",
        "drivers_by_cell",
        "location",
        182.0,
        262.0,
    );
    emit::close(&loc_srv, sim.t(270.0));
    emit::close(&loc_rpc, sim.t(276.0));

    let driver_count = if no_drivers {
        0
    } else {
        sim.rng.random_range(1i64..9)
    };
    match_srv
        .span()
        .set_attribute(KeyValue::new("matching.driver_count", driver_count));
    if no_drivers {
        emit::log(
            matching,
            &match_srv,
            Severity::Warn,
            format!("no drivers available near {city}"),
            vec![
                KeyValue::new("event.name", "matching.no_drivers"),
                KeyValue::new("geo.city", city),
            ],
        );
        emit::fail(&match_srv, "no drivers available", "NoDriversAvailable");
        emit::fail(&match_rpc, "no drivers available", "NoDriversAvailable");
    }
    emit::close(&match_srv, sim.t(300.0));
    emit::close(&match_rpc, sim.t(306.0));

    // trip-service -> pricing-service (surge quote via Redis cache).
    if !no_drivers {
        let price_rpc = grpc_client(
            sim,
            trip,
            &trip_srv,
            "rideshare.pricing.v1.PricingService",
            "QuoteFare",
            312.0,
        );
        let price_srv = grpc_server(
            sim,
            pricing,
            &price_rpc,
            "rideshare.pricing.v1.PricingService",
            "QuoteFare",
            318.0,
        );
        if surge_timeout {
            redis_timeout(sim, pricing, &price_srv, "GET", "surge:cell", 324.0, 414.0);
            emit::fail(&price_srv, "surge cache read timed out", "RedisTimeout");
            emit::fail(&price_rpc, "surge cache read timed out", "RedisTimeout");
            emit::log(
                pricing,
                &price_srv,
                Severity::Error,
                "surge cache read timed out after 90ms".to_string(),
                vec![
                    KeyValue::new("event.name", "cache.timeout"),
                    KeyValue::new("db.system", "redis"),
                ],
            );
        } else {
            redis_op(sim, pricing, &price_srv, "GET", "surge:cell", 324.0, 360.0);
            let fare = sim.rng.random_range(8.5f64..48.0);
            let surge = sim.rng.random_range(1.0f64..2.6);
            price_srv
                .span()
                .set_attribute(KeyValue::new("pricing.fare_usd", fare));
            price_srv
                .span()
                .set_attribute(KeyValue::new("pricing.surge_multiplier", surge));
        }
        emit::close(&price_srv, sim.t(416.0));
        emit::close(&price_rpc, sim.t(422.0));
    }

    if !failed {
        // Persist the trip.
        postgres_op(
            sim, trip, &trip_srv, "INSERT", "trips", "trips", 426.0, 482.0,
        );

        // Publish trip.requested to Kafka, consumed by notification-service.
        let producer = kafka_produce(
            sim,
            trip,
            &trip_srv,
            "trip.requested",
            &trip_id,
            488.0,
            520.0,
        );
        emit::log(
            trip,
            &trip_srv,
            Severity::Info,
            format!("trip {trip_id} created in {city}"),
            vec![
                KeyValue::new("event.name", "trip.created"),
                KeyValue::new("trip.id", trip_id.clone()),
                KeyValue::new("geo.city", city),
            ],
        );
        kafka_consume(
            sim,
            notify,
            &producer,
            "trip.requested",
            &trip_id,
            528.0,
            592.0,
            |sim, cx| {
                // notification-service -> FCM push.
                let push = emit::open(
                    notify,
                    "POST fcm.googleapis.com/v1/send",
                    SpanKind::Client,
                    cx,
                    sim.t(540.0),
                    vec![
                        KeyValue::new("http.request.method", "POST"),
                        KeyValue::new("server.address", "fcm.googleapis.com"),
                        KeyValue::new(
                            "url.full",
                            "https://fcm.googleapis.com/v1/projects/rideshare/messages:send",
                        ),
                        KeyValue::new("http.response.status_code", 200i64),
                    ],
                );
                emit::record(
                    &push,
                    &notify.instruments.http_client_duration,
                    sim.secs(540.0, 584.0),
                    &[
                        KeyValue::new("http.request.method", "POST"),
                        KeyValue::new("server.address", "fcm.googleapis.com"),
                        KeyValue::new("http.response.status_code", 200i64),
                    ],
                );
                emit::close(&push, sim.t(584.0));
            },
        );
        emit::close(&producer, sim.t(524.0));
    }

    // Statuses + RED metrics on the top of the stack.
    let status = if failed { 503 } else { 201 };
    finish_http(
        sim,
        gateway,
        &gw,
        "POST",
        "/v1/trips",
        status,
        28.0,
        600.0,
        failed,
    );
    emit::close(&trip_srv, sim.t(556.0));
    emit::close(&trip_rpc, sim.t(562.0));
    if failed {
        emit::fail(
            &trip_srv,
            "trip creation failed",
            if no_drivers {
                "NoDriversAvailable"
            } else {
                "RedisTimeout"
            },
        );
    }
    emit::close(&gw, sim.t(600.0));
    root.span()
        .set_attribute(KeyValue::new("http.response.status_code", status as i64));
    emit::close(&root, sim.t(640.0));
    gateway
        .instruments
        .http_active_requests
        .add(-1, &[KeyValue::new("http.route", "/v1/trips")]);
}

/// A driver handset streams a location update through the gateway to the
/// location-service, which writes to Redis. High-frequency, cheap, rarely
/// fails — the bread-and-butter background traffic.
fn rideshare_driver_ping(sim: &mut Sim) {
    const NS: &str = "rideshare";
    let driver = sim.svc(NS, "driver-app");
    let gateway = sim.svc(NS, "edge-gateway");
    let location = sim.svc(NS, "location-service");

    let driver_id = format!("driver_{:08}", sim.rng.random_range(1u64..400_000));

    let root = emit::open(
        driver,
        "POST /v1/locations",
        SpanKind::Client,
        &Context::new(),
        sim.t(0.0),
        vec![
            KeyValue::new("http.request.method", "POST"),
            KeyValue::new("url.full", "https://api.rideshare.com/v1/locations"),
            KeyValue::new("network.connection.type", *sim.pick(&["cell", "wifi"])),
            KeyValue::new("enduser.id", driver_id.clone()),
        ],
    );
    let gw = emit::open(
        gateway,
        "POST /v1/locations",
        SpanKind::Server,
        &root,
        sim.t(12.0),
        vec![
            KeyValue::new("http.request.method", "POST"),
            KeyValue::new("http.route", "/v1/locations"),
            KeyValue::new("server.address", "api.rideshare.com"),
            KeyValue::new("client.address", client_ip(sim)),
        ],
    );
    let loc_rpc = grpc_client(
        sim,
        gateway,
        &gw,
        "rideshare.location.v1.LocationService",
        "UpdateLocation",
        20.0,
    );
    let loc_srv = grpc_server(
        sim,
        location,
        &loc_rpc,
        "rideshare.location.v1.LocationService",
        "UpdateLocation",
        24.0,
    );
    redis_op(sim, location, &loc_srv, "GEOADD", "drivers:geo", 30.0, 48.0);
    emit::close(&loc_srv, sim.t(54.0));
    emit::close(&loc_rpc, sim.t(60.0));
    finish_http(
        sim,
        gateway,
        &gw,
        "POST",
        "/v1/locations",
        202,
        12.0,
        66.0,
        false,
    );
    emit::close(&gw, sim.t(66.0));
    root.span()
        .set_attribute(KeyValue::new("http.response.status_code", 202i64));
    emit::close(&root, sim.t(78.0));
}

// ---------------------------------------------------------------------------
// Online shop
// ---------------------------------------------------------------------------

/// A shopper checks out: web/mobile client → edge gateway → checkout-service,
/// which calls cart (Redis), inventory (PostgreSQL reservation) and payment
/// (external Stripe-like gateway), persists the order, publishes an
/// `order.placed` Kafka event consumed by shipping-service.
fn shop_checkout(sim: &mut Sim) {
    const NS: &str = "shop";
    let client_web = sim.chance(0.6);
    let client = if client_web {
        sim.svc(NS, "web-storefront")
    } else {
        sim.svc(NS, "shop-mobile")
    };
    let gateway = sim.svc(NS, "edge-gateway");
    let checkout = sim.svc(NS, "checkout-service");
    let cart = sim.svc(NS, "cart-service");
    let inventory = sim.svc(NS, "inventory-service");
    let payment = sim.svc(NS, "payment-service");
    let shipping = sim.svc(NS, "shipping-service");

    let order_id = format!("order_{:010}", sim.rng.random_range(1u64..9_000_000_000));
    let customer_id = format!("cust_{:08}", sim.rng.random_range(1u64..2_000_000));
    let items = sim.rng.random_range(1i64..6);
    let amount = sim.rng.random_range(12.0f64..480.0);
    let out_of_stock = sim.chance(0.05);
    let payment_declined = sim.chance(0.07);
    let failed = out_of_stock || payment_declined;

    let mut root_attrs = vec![
        KeyValue::new("http.request.method", "POST"),
        KeyValue::new("url.full", "https://shop.example.com/api/checkout"),
        KeyValue::new("server.address", "shop.example.com"),
        KeyValue::new("enduser.id", customer_id.clone()),
    ];
    if client_web {
        root_attrs.push(KeyValue::new("browser.mobile", false));
    } else {
        root_attrs.push(KeyValue::new("network.connection.type", "wifi"));
    }
    let root = emit::open(
        client,
        "POST /api/checkout",
        SpanKind::Client,
        &Context::new(),
        sim.t(0.0),
        root_attrs,
    );

    gateway
        .instruments
        .http_active_requests
        .add(1, &[KeyValue::new("http.route", "/api/checkout")]);
    let gw = emit::open(
        gateway,
        "POST /api/checkout",
        SpanKind::Server,
        &root,
        sim.t(24.0),
        vec![
            KeyValue::new("http.request.method", "POST"),
            KeyValue::new("http.route", "/api/checkout"),
            KeyValue::new("server.address", "shop.example.com"),
            KeyValue::new("client.address", client_ip(sim)),
        ],
    );
    emit::log(
        gateway,
        &gw,
        Severity::Info,
        format!("checkout for {customer_id}, {items} items"),
        vec![
            KeyValue::new("event.name", "checkout.received"),
            KeyValue::new("enduser.id", customer_id.clone()),
        ],
    );

    let co_rpc = grpc_client(
        sim,
        gateway,
        &gw,
        "shop.checkout.v1.CheckoutService",
        "PlaceOrder",
        32.0,
    );
    let co_srv = grpc_server(
        sim,
        checkout,
        &co_rpc,
        "shop.checkout.v1.CheckoutService",
        "PlaceOrder",
        38.0,
    );
    co_srv
        .span()
        .set_attribute(KeyValue::new("order.id", order_id.clone()));
    co_srv
        .span()
        .set_attribute(KeyValue::new("order.item_count", items));

    // checkout -> cart (Redis).
    let cart_rpc = grpc_client(
        sim,
        checkout,
        &co_srv,
        "shop.cart.v1.CartService",
        "GetCart",
        46.0,
    );
    let cart_srv = grpc_server(
        sim,
        cart,
        &cart_rpc,
        "shop.cart.v1.CartService",
        "GetCart",
        50.0,
    );
    redis_op(
        sim,
        cart,
        &cart_srv,
        "HGETALL",
        "cart:{customer}",
        56.0,
        92.0,
    );
    emit::close(&cart_srv, sim.t(98.0));
    emit::close(&cart_rpc, sim.t(104.0));

    // checkout -> inventory (PostgreSQL reservation).
    let inv_rpc = grpc_client(
        sim,
        checkout,
        &co_srv,
        "shop.inventory.v1.InventoryService",
        "Reserve",
        110.0,
    );
    let inv_srv = grpc_server(
        sim,
        inventory,
        &inv_rpc,
        "shop.inventory.v1.InventoryService",
        "Reserve",
        116.0,
    );
    if out_of_stock {
        postgres_op(
            sim,
            inventory,
            &inv_srv,
            "SELECT",
            "stock",
            "inventory",
            122.0,
            180.0,
        );
        emit::fail(
            &inv_srv,
            "insufficient stock for one or more items",
            "OutOfStock",
        );
        emit::fail(&inv_rpc, "insufficient stock", "OutOfStock");
        emit::log(
            inventory,
            &inv_srv,
            Severity::Warn,
            "reservation rejected: out of stock".to_string(),
            vec![
                KeyValue::new("event.name", "inventory.out_of_stock"),
                KeyValue::new("order.id", order_id.clone()),
            ],
        );
    } else {
        postgres_op(
            sim,
            inventory,
            &inv_srv,
            "UPDATE",
            "stock",
            "inventory",
            122.0,
            190.0,
        );
    }
    emit::close(&inv_srv, sim.t(196.0));
    emit::close(&inv_rpc, sim.t(202.0));

    // checkout -> payment -> external gateway.
    if !out_of_stock {
        let pay_rpc = grpc_client(
            sim,
            checkout,
            &co_srv,
            "shop.payment.v1.PaymentService",
            "Charge",
            210.0,
        );
        let pay_srv = grpc_server(
            sim,
            payment,
            &pay_rpc,
            "shop.payment.v1.PaymentService",
            "Charge",
            216.0,
        );
        let gw_span = emit::open(
            payment,
            "POST api.stripe.com/v1/charges",
            SpanKind::Client,
            &pay_srv,
            sim.t(224.0),
            vec![
                KeyValue::new("http.request.method", "POST"),
                KeyValue::new("server.address", "api.stripe.com"),
                KeyValue::new("url.full", "https://api.stripe.com/v1/charges"),
                KeyValue::new("peer.service", "stripe"),
                KeyValue::new(
                    "http.response.status_code",
                    if payment_declined { 402i64 } else { 200i64 },
                ),
            ],
        );
        emit::record(
            &gw_span,
            &payment.instruments.http_client_duration,
            sim.secs(224.0, 300.0),
            &[
                KeyValue::new("http.request.method", "POST"),
                KeyValue::new("server.address", "api.stripe.com"),
                KeyValue::new(
                    "http.response.status_code",
                    if payment_declined { 402i64 } else { 200i64 },
                ),
            ],
        );
        emit::close(&gw_span, sim.t(300.0));
        pay_srv
            .span()
            .set_attribute(KeyValue::new("payment.amount", amount));
        pay_srv
            .span()
            .set_attribute(KeyValue::new("payment.currency", "EUR"));
        if payment_declined {
            emit::fail(&pay_srv, "card declined by issuer", "CardDeclined");
            emit::fail(&pay_rpc, "card declined", "CardDeclined");
            emit::log(
                payment,
                &pay_srv,
                Severity::Error,
                format!("charge declined for order {order_id}"),
                vec![
                    KeyValue::new("event.name", "payment.declined"),
                    KeyValue::new("order.id", order_id.clone()),
                    KeyValue::new("payment.decline_code", "insufficient_funds"),
                ],
            );
        }
        emit::close(&pay_srv, sim.t(308.0));
        emit::close(&pay_rpc, sim.t(314.0));
    }

    if !failed {
        postgres_op(
            sim, checkout, &co_srv, "INSERT", "orders", "orders", 320.0, 372.0,
        );
        let producer = kafka_produce(
            sim,
            checkout,
            &co_srv,
            "order.placed",
            &order_id,
            378.0,
            410.0,
        );
        emit::log(
            checkout,
            &co_srv,
            Severity::Info,
            format!("order {order_id} placed, {amount:.2} EUR"),
            vec![
                KeyValue::new("event.name", "order.placed"),
                KeyValue::new("order.id", order_id.clone()),
                KeyValue::new("order.amount", amount),
            ],
        );
        kafka_consume(
            sim,
            shipping,
            &producer,
            "order.placed",
            &order_id,
            418.0,
            486.0,
            |sim, cx| {
                postgres_op(
                    sim,
                    shipping,
                    cx,
                    "INSERT",
                    "shipments",
                    "shipping",
                    432.0,
                    478.0,
                );
            },
        );
        emit::close(&producer, sim.t(414.0));
    }

    let status = if payment_declined {
        402
    } else if out_of_stock {
        409
    } else {
        201
    };
    finish_http(
        sim,
        gateway,
        &gw,
        "POST",
        "/api/checkout",
        status,
        24.0,
        500.0,
        failed,
    );
    if failed {
        emit::fail(
            &co_srv,
            "order could not be placed",
            if out_of_stock {
                "OutOfStock"
            } else {
                "CardDeclined"
            },
        );
    }
    emit::close(&co_srv, sim.t(456.0));
    emit::close(&co_rpc, sim.t(462.0));
    emit::close(&gw, sim.t(500.0));
    root.span()
        .set_attribute(KeyValue::new("http.response.status_code", status as i64));
    emit::close(&root, sim.t(540.0));
    gateway
        .instruments
        .http_active_requests
        .add(-1, &[KeyValue::new("http.route", "/api/checkout")]);
}

/// A shopper browses: search-service (with a recommendation-service sidecar
/// call) and catalog-service reads. Read-heavy, cache-friendly background load.
fn shop_browse(sim: &mut Sim) {
    const NS: &str = "shop";
    let client_web = sim.chance(0.7);
    let client = if client_web {
        sim.svc(NS, "web-storefront")
    } else {
        sim.svc(NS, "shop-mobile")
    };
    let gateway = sim.svc(NS, "edge-gateway");
    let search = sim.svc(NS, "search-service");
    let catalog = sim.svc(NS, "catalog-service");
    let recommend = sim.svc(NS, "recommendation-service");

    let query = *sim.pick(&[
        "running shoes",
        "usb-c cable",
        "coffee beans",
        "desk lamp",
        "wireless mouse",
    ]);

    let root = emit::open(
        client,
        "GET /api/search",
        SpanKind::Client,
        &Context::new(),
        sim.t(0.0),
        vec![
            KeyValue::new("http.request.method", "GET"),
            KeyValue::new(
                "url.full",
                format!(
                    "https://shop.example.com/api/search?q={}",
                    query.replace(' ', "+")
                ),
            ),
            KeyValue::new("url.query", format!("q={query}")),
        ],
    );
    let gw = emit::open(
        gateway,
        "GET /api/search",
        SpanKind::Server,
        &root,
        sim.t(14.0),
        vec![
            KeyValue::new("http.request.method", "GET"),
            KeyValue::new("http.route", "/api/search"),
            KeyValue::new("client.address", client_ip(sim)),
        ],
    );

    let s_rpc = grpc_client(
        sim,
        gateway,
        &gw,
        "shop.search.v1.SearchService",
        "Search",
        20.0,
    );
    let s_srv = grpc_server(
        sim,
        search,
        &s_rpc,
        "shop.search.v1.SearchService",
        "Search",
        24.0,
    );
    // Elasticsearch-style query modeled as a db.client span.
    let es = emit::open(
        search,
        "search products",
        SpanKind::Client,
        &s_srv,
        sim.t(30.0),
        vec![
            KeyValue::new("db.system", "elasticsearch"),
            KeyValue::new("db.operation.name", "_search"),
            KeyValue::new("db.namespace", "products"),
            KeyValue::new("db.query.text", format!("GET /products/_search q={query}")),
        ],
    );
    emit::record(
        &es,
        &search.instruments.db_client_duration,
        sim.secs(30.0, 88.0),
        &[
            KeyValue::new("db.system", "elasticsearch"),
            KeyValue::new("db.operation.name", "_search"),
        ],
    );
    emit::close(&es, sim.t(88.0));

    // catalog hydrate.
    let c_rpc = grpc_client(
        sim,
        search,
        &s_srv,
        "shop.catalog.v1.CatalogService",
        "GetProducts",
        94.0,
    );
    let c_srv = grpc_server(
        sim,
        catalog,
        &c_rpc,
        "shop.catalog.v1.CatalogService",
        "GetProducts",
        98.0,
    );
    redis_op(sim, catalog, &c_srv, "MGET", "product:{ids}", 104.0, 128.0);
    emit::close(&c_srv, sim.t(134.0));
    emit::close(&c_rpc, sim.t(140.0));

    // recommendations sidecar.
    let r_rpc = grpc_client(
        sim,
        search,
        &s_srv,
        "shop.reco.v1.RecommendationService",
        "Related",
        146.0,
    );
    let r_srv = grpc_server(
        sim,
        recommend,
        &r_rpc,
        "shop.reco.v1.RecommendationService",
        "Related",
        150.0,
    );
    emit::close(&r_srv, sim.t(198.0));
    emit::close(&r_rpc, sim.t(204.0));

    emit::close(&s_srv, sim.t(210.0));
    emit::close(&s_rpc, sim.t(216.0));
    finish_http(
        sim,
        gateway,
        &gw,
        "GET",
        "/api/search",
        200,
        14.0,
        224.0,
        false,
    );
    emit::close(&gw, sim.t(224.0));
    root.span()
        .set_attribute(KeyValue::new("http.response.status_code", 200i64));
    emit::close(&root, sim.t(246.0));
}

// ---------------------------------------------------------------------------
// Shared span/metric building blocks
// ---------------------------------------------------------------------------

/// Opens a gRPC client span and records the RPC-client latency metric.
fn grpc_client(
    sim: &mut Sim,
    svc: &ServiceTelemetry,
    parent: &Context,
    service: &str,
    method: &str,
    start_ms: f64,
) -> Context {
    let cx = emit::open(
        svc,
        format!("{service}/{method}"),
        SpanKind::Client,
        parent,
        sim.t(start_ms),
        vec![
            KeyValue::new("rpc.system", "grpc"),
            KeyValue::new("rpc.service", service.to_string()),
            KeyValue::new("rpc.method", method.to_string()),
            KeyValue::new("network.transport", "tcp"),
        ],
    );
    emit::record(
        &cx,
        &svc.instruments.rpc_client_duration,
        sim.secs(start_ms, start_ms + 200.0),
        &[
            KeyValue::new("rpc.system", "grpc"),
            KeyValue::new("rpc.service", service.to_string()),
            KeyValue::new("rpc.method", method.to_string()),
        ],
    );
    cx
}

/// Opens the matching gRPC server span on the callee.
fn grpc_server(
    sim: &mut Sim,
    svc: &ServiceTelemetry,
    parent: &Context,
    service: &str,
    method: &str,
    start_ms: f64,
) -> Context {
    emit::open(
        svc,
        format!("{service}/{method}"),
        SpanKind::Server,
        parent,
        sim.t(start_ms),
        vec![
            KeyValue::new("rpc.system", "grpc"),
            KeyValue::new("rpc.service", service.to_string()),
            KeyValue::new("rpc.method", method.to_string()),
            KeyValue::new("rpc.grpc.status_code", 0i64),
        ],
    )
}

/// A successful Redis operation (client span + db latency metric).
fn redis_op(
    sim: &mut Sim,
    svc: &ServiceTelemetry,
    parent: &Context,
    op: &str,
    key: &str,
    start_ms: f64,
    end_ms: f64,
) {
    let cx = emit::open(
        svc,
        format!("{op} {key}"),
        SpanKind::Client,
        parent,
        sim.t(start_ms),
        vec![
            KeyValue::new("db.system", "redis"),
            KeyValue::new("db.operation.name", op.to_string()),
            KeyValue::new("db.query.text", format!("{op} {key}")),
            KeyValue::new("server.address", "redis.internal"),
            KeyValue::new("network.peer.port", 6379i64),
        ],
    );
    emit::record(
        &cx,
        &svc.instruments.db_client_duration,
        sim.secs(start_ms, end_ms),
        &[
            KeyValue::new("db.system", "redis"),
            KeyValue::new("db.operation.name", op.to_string()),
        ],
    );
    emit::close(&cx, sim.t(end_ms));
}

/// A Redis operation that times out (errored client span).
fn redis_timeout(
    sim: &mut Sim,
    svc: &ServiceTelemetry,
    parent: &Context,
    op: &str,
    key: &str,
    start_ms: f64,
    end_ms: f64,
) {
    let cx = emit::open(
        svc,
        format!("{op} {key}"),
        SpanKind::Client,
        parent,
        sim.t(start_ms),
        vec![
            KeyValue::new("db.system", "redis"),
            KeyValue::new("db.operation.name", op.to_string()),
            KeyValue::new("server.address", "redis.internal"),
            KeyValue::new("error.type", "RedisTimeout"),
        ],
    );
    emit::fail(&cx, "redis read timed out", "RedisTimeout");
    emit::record(
        &cx,
        &svc.instruments.db_client_duration,
        sim.secs(start_ms, end_ms),
        &[
            KeyValue::new("db.system", "redis"),
            KeyValue::new("db.operation.name", op.to_string()),
            KeyValue::new("error.type", "RedisTimeout"),
        ],
    );
    emit::close(&cx, sim.t(end_ms));
}

/// A PostgreSQL operation (client span + db latency metric).
// Emitter helper: distinct scalar args (span identity + timing); no coherent
// struct to group them, and bundling would only obscure the call sites.
#[allow(clippy::too_many_arguments)]
fn postgres_op(
    sim: &mut Sim,
    svc: &ServiceTelemetry,
    parent: &Context,
    op: &str,
    table: &str,
    db: &str,
    start_ms: f64,
    end_ms: f64,
) {
    let cx = emit::open(
        svc,
        format!("{op} {table}"),
        SpanKind::Client,
        parent,
        sim.t(start_ms),
        vec![
            KeyValue::new("db.system", "postgresql"),
            KeyValue::new("db.operation.name", op.to_string()),
            KeyValue::new("db.collection.name", table.to_string()),
            KeyValue::new("db.namespace", db.to_string()),
            KeyValue::new("db.query.text", format!("{op} ... FROM {table}")),
            KeyValue::new("server.address", format!("{db}.db.internal")),
            KeyValue::new("network.peer.port", 5432i64),
        ],
    );
    emit::record(
        &cx,
        &svc.instruments.db_client_duration,
        sim.secs(start_ms, end_ms),
        &[
            KeyValue::new("db.system", "postgresql"),
            KeyValue::new("db.operation.name", op.to_string()),
            KeyValue::new("db.collection.name", table.to_string()),
        ],
    );
    emit::close(&cx, sim.t(end_ms));
}

/// A Kafka producer span (publish) + `messaging.client.sent.messages` counter.
fn kafka_produce(
    sim: &mut Sim,
    svc: &ServiceTelemetry,
    parent: &Context,
    topic: &str,
    key: &str,
    start_ms: f64,
    _end_ms: f64,
) -> Context {
    let partition = sim.rng.random_range(0i64..12);
    let attrs = |extra: KeyValue| {
        vec![
            KeyValue::new("messaging.system", "kafka"),
            KeyValue::new("messaging.destination.name", topic.to_string()),
            KeyValue::new("messaging.operation.name", "publish"),
            KeyValue::new("messaging.operation.type", "send"),
            KeyValue::new("messaging.kafka.message.key", key.to_string()),
            KeyValue::new("messaging.destination.partition.id", partition),
            extra,
        ]
    };
    let cx = emit::open(
        svc,
        format!("publish {topic}"),
        SpanKind::Producer,
        parent,
        sim.t(start_ms),
        attrs(KeyValue::new("server.address", "kafka.internal")),
    );
    svc.instruments.messages_sent.add(
        1,
        &[
            KeyValue::new("messaging.system", "kafka"),
            KeyValue::new("messaging.destination.name", topic.to_string()),
        ],
    );
    cx
}

/// A Kafka consumer span (process) parented to the producer, with a nested
/// unit of work supplied by `body`, plus consume counter and process-duration.
// Emitter helper: distinct scalar args (span identity + timing); no coherent
// struct to group them, and bundling would only obscure the call sites.
#[allow(clippy::too_many_arguments)]
fn kafka_consume(
    sim: &mut Sim,
    svc: &ServiceTelemetry,
    producer_cx: &Context,
    topic: &str,
    key: &str,
    start_ms: f64,
    end_ms: f64,
    body: impl FnOnce(&mut Sim, &Context),
) {
    let cx = emit::open(
        svc,
        format!("process {topic}"),
        SpanKind::Consumer,
        producer_cx,
        sim.t(start_ms),
        vec![
            KeyValue::new("messaging.system", "kafka"),
            KeyValue::new("messaging.destination.name", topic.to_string()),
            KeyValue::new("messaging.operation.name", "process"),
            KeyValue::new("messaging.operation.type", "process"),
            KeyValue::new("messaging.kafka.message.key", key.to_string()),
            KeyValue::new("messaging.consumer.group.name", format!("{}-cg", svc.name)),
        ],
    );
    svc.instruments.messages_consumed.add(
        1,
        &[
            KeyValue::new("messaging.system", "kafka"),
            KeyValue::new("messaging.destination.name", topic.to_string()),
        ],
    );
    body(sim, &cx);
    emit::record(
        &cx,
        &svc.instruments.message_process_duration,
        sim.secs(start_ms, end_ms),
        &[
            KeyValue::new("messaging.system", "kafka"),
            KeyValue::new("messaging.destination.name", topic.to_string()),
        ],
    );
    emit::close(&cx, sim.t(end_ms));
}

/// Sets the HTTP response status on a server span and records the
/// `http.server.request.duration` RED metric (with an error.type on failure).
// Emitter helper: distinct scalar args (span identity + timing + status); no
// coherent struct to group them, and bundling would only obscure call sites.
#[allow(clippy::too_many_arguments)]
fn finish_http(
    sim: &mut Sim,
    svc: &ServiceTelemetry,
    cx: &Context,
    method: &str,
    route: &str,
    status: u16,
    start_ms: f64,
    end_ms: f64,
    failed: bool,
) {
    cx.span()
        .set_attribute(KeyValue::new("http.response.status_code", status as i64));
    let mut attrs = vec![
        KeyValue::new("http.request.method", method.to_string()),
        KeyValue::new("http.route", route.to_string()),
        KeyValue::new("http.response.status_code", status as i64),
    ];
    if failed {
        attrs.push(KeyValue::new("error.type", status.to_string()));
    }
    emit::record(
        cx,
        &svc.instruments.http_server_duration,
        sim.secs(start_ms, end_ms),
        &attrs,
    );
}

/// A random-looking but stable client IP for `client.address`.
fn client_ip(sim: &mut Sim) -> String {
    format!(
        "{}.{}.{}.{}",
        sim.rng.random_range(1i64..223),
        sim.rng.random_range(0i64..255),
        sim.rng.random_range(0i64..255),
        sim.rng.random_range(1i64..254),
    )
}

#[cfg(test)]
mod tests {
    //! Regression tests for #797: produced spans must carry realistic
    //! simulated durations — non-zero roots, children nested inside their
    //! parent's window, and no span ending in the future.

    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};

    use opentelemetry::trace::{SpanId, SpanKind, TraceId};
    use opentelemetry_sdk::trace::SpanData;

    use super::run;
    use crate::fleet::Fleet;
    use crate::topology;

    /// Runs every estate's scenarios repeatedly against in-memory exporters
    /// and returns all finished spans. Scenario choice and timing scale are
    /// random, so the assertions below must hold for every possible draw.
    fn produce_spans() -> Vec<SpanData> {
        let estates = topology::estates("all");
        let (fleet, exporter) = Fleet::build_in_memory(&estates);
        let mut rng = rand::rng();
        for _ in 0..25 {
            for estate in &estates {
                run(&fleet, &mut rng, estate.namespace);
            }
        }
        exporter
            .get_finished_spans()
            .expect("in-memory exporter should return finished spans")
    }

    fn duration_of(span: &SpanData) -> Duration {
        span.end_time
            .duration_since(span.start_time)
            .unwrap_or_else(|_| panic!("span '{}' ends before it starts", span.name))
    }

    #[test]
    fn every_span_has_a_positive_duration() {
        let spans = produce_spans();
        assert!(!spans.is_empty(), "scenarios should emit spans");
        for span in &spans {
            assert!(
                duration_of(span) > Duration::ZERO,
                "span '{}' has zero duration",
                span.name
            );
        }
    }

    #[test]
    fn root_spans_have_realistic_durations() {
        let spans = produce_spans();
        let roots: Vec<_> = spans
            .iter()
            .filter(|s| s.parent_span_id == SpanId::INVALID)
            .collect();
        assert!(!roots.is_empty(), "scenarios should emit root spans");
        for root in roots {
            let duration = duration_of(root);
            assert!(
                duration >= Duration::from_millis(5),
                "root span '{}' lasts only {duration:?} — traces would list as ~0",
                root.name
            );
            assert!(
                duration <= Duration::from_secs(5),
                "root span '{}' lasts an implausible {duration:?}",
                root.name
            );
        }
    }

    #[test]
    fn spans_never_end_in_the_future() {
        let spans = produce_spans();
        let now = SystemTime::now();
        for span in &spans {
            assert!(
                span.end_time <= now,
                "span '{}' has an end timestamp in the future",
                span.name
            );
        }
    }

    #[test]
    fn child_spans_nest_inside_their_parents_window() {
        let spans = produce_spans();
        let by_id: HashMap<(TraceId, SpanId), &SpanData> = spans
            .iter()
            .map(|s| ((s.span_context.trace_id(), s.span_context.span_id()), s))
            .collect();
        for span in &spans {
            if span.parent_span_id == SpanId::INVALID {
                continue;
            }
            // Consumer spans model asynchronous message processing and may
            // legitimately start after their Producer parent has ended.
            if span.span_kind == SpanKind::Consumer {
                continue;
            }
            let parent = by_id
                .get(&(span.span_context.trace_id(), span.parent_span_id))
                .unwrap_or_else(|| panic!("span '{}' has no exported parent", span.name));
            assert!(
                span.start_time >= parent.start_time,
                "span '{}' starts before its parent '{}'",
                span.name,
                parent.name
            );
            assert!(
                span.end_time <= parent.end_time,
                "span '{}' ends after its parent '{}'",
                span.name,
                parent.name
            );
        }
    }
}
