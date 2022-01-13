defmodule Wadm.Api.ApiServer do
  @moduledoc """
  Exposes the API for interacting with wadm over the NATS subject wadm.api.>
  The following is a list of the operations available in this API (minus the prefix):

  * `apps.get` - Retrieves a summary of the application models stored by this wadm cluster. This will include status information
  * `app.put` - Adds a new application model
  * `app.del` - Deletes an application model
  * `app.deploy` - Deploys an application to a given lattice
  * `app.undeploy` - Stops managing an application in the given lattice. Non-destructive.
  * `app.details` - Retrieves a full set of details about an application, including the model
  * `app.history` - Retrieves an audit history for a particular application
  """
  require Logger
  use Gnat.Server

  alias Wadm.Model.{Validator, AppSpec}

  def request(%{topic: topic, body: body}) do
    topic
    |> String.split(".")
    # wadm
    |> List.delete_at(0)
    # api
    |> List.delete_at(0)
    |> List.to_tuple()
    |> handle_request(body)
  end

  # TODO just mocking this data up for now.
  # will make it real once we like the shapes
  defp handle_request({"apps", "get"}, _body) do
    response =
      [
        %{
          name: "App name",
          version: "0.1.0",
          description: "My app",
          deployment_status: [
            %{
              lattice: "default",
              # :fully_deployed, :fail
              status: :partially_deployed
            }
          ]
        }
      ]
      |> Jason.encode!()

    {:reply, response}
  end

  defp handle_request({"app", "put"}, body) do
    # input here is a JSON version of the OAM model
    with {:ok, decoded} <- Jason.decode(body),
         {:ok, model} <- AppSpec.from_map(decoded),
         :ok <- Validator.validate_appspec(model) do
      # TODO - store model
      {:reply,
       %{
         accepted: true,
         error: nil
       }
       |> Jason.encode!()}
    else
      {:error, e} ->
        {:reply,
         %{
           accepted: false,
           error: "Failed to put application model: #{inspect(e)}"
         }
         |> Jason.encode!()}
    end
  end

  defp handle_request({"app", "del"}, body) do
    with {:ok, _request} <- Jason.decode(body) do
      # TODO - remove from store
      # %{
      # "name" => "name",
      # "versions" => []
      # }
      # if the list of versions is empty, _all_ versions will be removed
      # TODO - stop all deployment monitors for the list of models to delete
      {:reply,
       %{
         accepted: true,
         error: nil
       }
       |> Jason.encode!()}
    else
      {:error, e} ->
        {:reply,
         %{
           accepted: false,
           error: "Failed to delete application model: #{inspect(e)}"
         }
         |> Jason.encode!()}
    end
  end

  defp handle_request({"app", "deploy"}, body) do
    with {:ok, _request} <- Jason.decode(body) do
      # TODO - validate existence of model
      # TODO - start deployment monitor
      # %{ "name" => "n", "version" => "v", "lattice" => "default" }
      {:reply, %{accepted: true, error: nil} |> Jason.encode!()}
    else
      {:error, e} ->
        {:reply,
         %{
           accepted: false,
           error: "Failed to deploy application: #{inspect(e)}"
         }
         |> Jason.encode!()}
    end
  end

  defp handle_request({"app", "undeploy"}, body) do
    with {:ok, _request} <- Jason.decode(body) do
      # TODO - validate existence of model
      # TODO - stop deployment monitor (idempotent)
      # %{ "name" => "n", "version" => "v", "lattice" => "default" }
      {:reply, %{accepted: true, error: nil} |> Jason.encode!()}
    else
      {:error, e} ->
        {:reply,
         %{
           accepted: false,
           error: "Failed to undeploy application: #{inspect(e)}"
         }
         |> Jason.encode!()}
    end
  end

  defp handle_request({"app", "details"}, body) do
    with {:ok, _request} <- Jason.decode(body) do
      # TODO - load from store
      # this should return the OAM model that was sent in a "push" and NOT
      # the internal %AppSpec{} struct
      {:reply,
       %{
         placeholder: true
       }
       |> Jason.encode!()}
    else
      {:error, e} ->
        {:reply,
         %{
           error: "Failed to query application details: #{inspect(e)}"
         }
         |> Jason.encode!()}
    end
  end

  defp handle_request({"app", "history"}, body) do
    with {:ok, _request} <- Jason.decode(body) do
      # %{
      # "name" => "n", "version" => "v", "lattice" => "default"
      # }
      # audit log history
      # - actions taken (emissions from reconciler)
      # - actions failed
      {:reply, [] |> Jason.encode!()}
    else
      {:error, e} ->
        {:reply,
         %{
           error: "Failed to obtain application history: #{inspect(e)}"
         }
         |> Jason.encode!()}
    end
  end
end
