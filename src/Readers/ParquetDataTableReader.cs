using System.Data;

namespace Bau.Libraries.LibParquetFiles.Readers;

/// <summary>
///     Lector de un archivo Parquet sobre un DataTable
/// </summary>
public class ParquetDataTableReader
{
	/// <summary>
	///		Carga una página de un archivo en un dataTable
	/// </summary>
	public async Task<(DataTable table, long totalRecordCount)> LoadAsync(string fileName, int page, int recordsPerPage, bool countRecords, 
																		  ParquetFiltersCollection? filters, CancellationToken cancellationToken)
	{
		long record = 0;
		int offset = (page - 1) * recordsPerPage;
		DataTable table = new DataTable();
		bool end = false;

			// Lee los datos
			using (ParquetDataReader reader = new ParquetDataReader())
			{
				// Abre el archivo
				await reader.OpenAsync(fileName, cancellationToken);
				// Lee los registros
				while (await reader.ReadAsync(cancellationToken) && !end && !cancellationToken.IsCancellationRequested)
				{
					// Añade el esquema a la tabla
					if (table.Columns.Count == 0)
						AddSchema(table, reader);
					// Sólo se tiene en cuenta la fila si estamos en el filtro
					if (IsAtFilter(reader, filters))
					{
						// Añade la fila a la tabla
						if (record >= offset && record < offset + recordsPerPage)
							AddRow(table, reader);
						// Incrementa el registro (sólo si estamos en el filtro para contabilizar correctamente el número de registros)
						record++;
					}
					// Comprueba si se deben contar todos los registros
					end = !countRecords && record >= offset + recordsPerPage;
				}
			}
			// Devuelve la tabla de datos
			return (table, record);
	}

	/// <summary>
	///		Comprueba si se debe añadir el registro actual a la salida teniendo en cuentra el filtro
	/// </summary>
	private bool IsAtFilter(ParquetDataReader reader, ParquetFiltersCollection? filters)
	{
		if (filters is null || filters.Count == 0)
			return true;
		else
		{
			// Evalúa las condiciones sobre los campos
			for (int index = 0; index < reader.FieldCount; index++)
				if (!filters.Evaluate(reader.GetName(index), reader[index]))
					return false;
			// Si ha llegado hasta aquí es porque cumple con todas las condiciones
			return true;
		}
	}

	/// <summary>
	///		Añade el esquema a la tabla
	/// </summary>
	private void AddSchema(DataTable table, ParquetDataReader reader)
	{
		for (int index = 0; index < reader.FieldCount; index++)
			table.Columns.Add(reader.GetName(index), reader.GetValue(index).GetType());
	}

	/// <summary>
	///		Añade una fila a la tabla
	/// </summary>
	private void AddRow(DataTable table, ParquetDataReader reader)
	{
		DataRow row = table.NewRow();

			// Añade las columnas
			foreach (DataColumn column in row.Table.Columns)
				row[column] = reader[column.ColumnName];
			// Añade la fila a la tabla
			table.Rows.Add(row);
	}
}
