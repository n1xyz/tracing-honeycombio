# tracing-honeycombio

To document later:

Invariants

- chosen subscriber must implement `LookupSpan`, assign IDs sanely, etc.
  `tracing_subscriber::registry` is known to work.
- no adding layers that mess with span IDs (it's not possible for us to implement `on_id_change` method on trait `Layer`)
- all fields added to spans/events must never fail to serialize (this is enforced by tracing's type system, because it only allows primitives)
- the behavior when you try to add fields that have same name as reserved ones is undefined.
  sometimes you will get overridden, other times both values are included and it is up to honeycomb to decide what to do with it.
  we don't bother checking for this

- flake
-
