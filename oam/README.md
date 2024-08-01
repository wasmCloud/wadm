# wadm Open Application Model

The wasmCloud Application Deployment Manager uses the [Open Application Model](https://oam.dev) to define application specifications. Because this specification is extensible and _platform agnostic_, it makes for an ideal way to represent applications with metadata specific to wasmCloud.

## wasmCloud OAM Components

The following is a list of the `component`s wasmCloud has added to the model.

- `component` - A WebAssembly component
- `provider` - A capability provider

## wasmCloud OAM Traits

The following is a list of the `traits` wasmCloud has added via customization to its application model.

- `spreadscaler` - Defines the spread of instances of a particular entity across multiple hosts with affinity requirements
- `link` - A link definition that describes a link between a component and a capability provider or a component and another component

## JSON Schema

A JSON schema is automatically generated from our Rust structures and is at the root of the repository: [oam.schema.json](../oam.schema.json).

## Example Application YAML

The following is an example YAML file describing an application

```yaml
apiVersion: core.oam.dev/v1beta1
kind: Application
metadata:
  name: my-example-app
  annotations:
    description: 'This is my app revision 2'
spec:
  components:
    - name: userinfo
      type: component
      properties:
        image: wasmcloud.azurecr.io/fake:1
      traits:
        - type: spreadscaler
          properties:
            instances: 4
            spread:
              - name: eastcoast
                requirements:
                  zone: us-east-1
                weight: 80
              - name: westcoast
                requirements:
                  zone: us-west-1
                weight: 20

    - name: webcap
      type: capability
      properties:
        image: wasmcloud.azurecr.io/httpserver:0.13.1
      traits:
        - type: link
          properties:
            target:
              name: userinfo
              config: []
            namespace: wasi
            package: http
            interfaces:
              - incoming-handler
            source:
              config: []

    - name: ledblinky
      type: capability
      properties:
        image: wasmcloud.azurecr.io/ledblinky:0.0.1
      traits:
        - type: spreadscaler
          properties:
            instances: 1
            spread:
              - name: haslights
                requirements:
                  ledenabled: 'true'
                  # default weight is 100
```
