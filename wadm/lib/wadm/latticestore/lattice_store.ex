defmodule Wadm.LatticeStore do
  @moduledoc false
  require Record
  use Mnesiac.Store

  Record.defrecord(
    :lattice,
    __MODULE__,
    # mandatory
    id: nil,
    prefix: nil,
    data: nil
  )

  @type lattice ::
          record(
            :lattice,
            id: String.t(),
            prefix: String.t(),
            data: binary()
          )

  @impl true
  def store_options,
    do: [
      attributes: lattice() |> lattice() |> Keyword.keys(),
      index: [:prefix],
      disc_copies: [node()]
    ]

  @impl true
  def copy_store do
    :mnesia.add_table_copy(__MODULE__, node(), :disc_copies)
  end
end
