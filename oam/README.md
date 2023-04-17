# wadm Open Application Model
The wasmCloud Application Deployment Manager uses the [Open Application Model](https://oam.dev) to define application specifications. Because this specification is extensible and _platform agnostic_, it makes for an ideal way to represent applications with metadata specific to wasmCloud.

## wasmCloud OAM Components
The following is a list of the `component`s wasmCloud has added to the model.
* `actor` - An actor
* `provider` - A capability provider

## wasmCloud OAM Traits
The following is a list of the `traits` wasmCloud has added via customization to its application model.
* `spreadscaler` - Defines the spread of instances of a particular entity across multiple hosts with affinity requirements
* `linkdef` - A link definition that describes a link between an actor and a capability provider

## Example Application YAML
The following is an example YAML file describing an ALC application

```yaml
apiVersion: core.oam.dev/v1beta1
kind: Application
metadata:
  name: my-example-app
  annotations:
    version: v0.0.1
    description: "This is my app"
spec:
  components:
    - name: userinfo
      type: actor
      properties:
        image: wasmcloud.azurecr.io/fake:1
      traits:
        - type: spreadscaler
          properties:
            replicas: 4
            spread:
              - name: eastcoast
                requirements:
                  zone: us-east-1
                weight: 80
              - name: westcoast
                requirements:
                  zone: us-west-1
                weight: 20
        - type: linkdef
          properties:
            target: webcap
            values:
              port: "8080"

    - name: webcap
      type: capability
      properties:
        contract: wasmcloud:httpserver
        image: wasmcloud.azurecr.io/httpserver:0.13.1
        link_name: default

    - name: ledblinky
      type: capability
      properties:
        image: wasmcloud.azurecr.io/ledblinky:0.0.1
        contract: wasmcloud:blinkenlights
        # default link name is "default"
      traits:
        - type: spreadscaler
          properties:
            replicas: 1
            spread:
              - name: haslights
                requirements:
                  ledenabled: "true"
                # default weight is 100
```
