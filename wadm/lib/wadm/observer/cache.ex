defmodule Wadm.Observer.Cache do
  @callback write_lattice(prefix :: LatticeObserver.Observed.Lattice.t()) :: nil
  @callback load_lattice(lattice :: binary) ::
              any
end
