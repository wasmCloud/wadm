defmodule Wadm.Observer.Cache do
  @callback write_lattice(prefix :: String.t()) :: nil
  @callback load_lattice(lattice :: LatticeObserver.Observed.Lattice) ::
              LatticeObserver.Observed.Lattice
end
