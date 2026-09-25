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
	/// <remarks>
	///		Si <paramref name="countRecords"/> es true, <c>totalRecordCount</c> es el número real de filas del
	///	archivo (se lee hasta el final). Si es false, la lectura se detiene en cuanto la página solicitada está
	///	completa y <c>totalRecordCount</c> es exactamente el número de filas recorridas hasta ese punto (salvo
	///	que el archivo tenga menos filas que las necesarias para completar la página, caso en el que se lee hasta
	///	el final de todas formas)
	/// </remarks>
	public async Task<(DataTable table, long totalRecordCount)> LoadAsync(string fileName, int page, int recordsPerPage, bool countRecords, CancellationToken cancellationToken)
	{
		ArgumentOutOfRangeException.ThrowIfLessThan(page, 1);
		ArgumentOutOfRangeException.ThrowIfNegativeOrZero(recordsPerPage);

		long record = 0;
		int offset = (page - 1) * recordsPerPage;
		DataTable table = new DataTable();

			// Lee los datos
			using (ParquetDataReader reader = new ParquetDataReader())
			{
				// Abre el archivo y añade el esquema a la tabla, incluso si no tiene ninguna fila
				await reader.OpenAsync(fileName, cancellationToken);
				AddSchema(table, reader);
				// Lee los registros
				while (await reader.ReadAsync(cancellationToken))
				{
					// Si se ha solicitado la cancelación, lanza en lugar de devolver una página parcial en silencio
					cancellationToken.ThrowIfCancellationRequested();
					// Añade la fila a la tabla si cae dentro de la página solicitada
					if (record >= offset && record < offset + recordsPerPage)
						AddRow(table, reader);
					// Incrementa el registro
					record++;
					// Si no hace falta contar todos los registros y la página ya está completa, deja de leer
					if (!countRecords && record >= offset + recordsPerPage)
						break;
				}
			}
			// Devuelve la tabla de datos
			return (table, record);
	}

	/// <summary>
	///		Añade el esquema a la tabla
	/// </summary>
	private void AddSchema(DataTable table, ParquetDataReader reader)
	{
		for (int index = 0; index < reader.FieldCount; index++)
			table.Columns.Add(reader.GetName(index), reader.GetFieldType(index));
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