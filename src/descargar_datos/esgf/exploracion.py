"""Script de exploracion para descubrir parametros validos en ESGF."""

from __future__ import annotations

from intake_esgf import ESGFCatalog


def main() -> None:
    """Ejecuta exploracion basica para EC-Earth3 en ESGF."""
    print("Buscando EC-Earth3 + dcppA-hindcast (sin filtros)...")
    cat = ESGFCatalog()
    cat.search(
        source_id="EC-Earth3",
        experiment_id="dcppA-hindcast",
        variable_id="pr",
        table_id="Amon",
    )

    if len(cat.df) == 0:
        print("Sin resultados. Probando sin variable_id ni table_id...")
        cat2 = ESGFCatalog()
        cat2.search(
            source_id="EC-Earth3",
            experiment_id="dcppA-hindcast",
        )
        if len(cat2.df) == 0:
            print("Sin resultados en absoluto para EC-Earth3 / dcppA-hindcast.")
            return
        cols = [
            c
            for c in [
                "variant_label",
                "grid_label",
                "sub_experiment_id",
                "variable_id",
                "table_id",
            ]
            if c in cat2.df.columns
        ]
        print(f"\nResultados encontrados ({len(cat2.df)} filas).")
        print("Valores unicos disponibles:")
        print(cat2.df[cols].drop_duplicates().to_string())
        return

    cols = [
        c
        for c in ["variant_label", "grid_label", "sub_experiment_id", "variable_id"]
        if c in cat.df.columns
    ]
    print(f"\nResultados encontrados ({len(cat.df)} filas).")
    print("Valores unicos disponibles:")
    print(cat.df[cols].drop_duplicates().to_string())


if __name__ == "__main__":
    main()
