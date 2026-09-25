using System.Data;

namespace ParquetFiles.Tests.MotherObject;

/// <summary>
///		Atajos para construir <see cref="DataTable"/> de prueba
/// </summary>
internal static class DataTableBuilder
{
	/// <summary>
	///		Crea una tabla con una única columna del tipo indicado y sus valores (que pueden incluir null)
	/// </summary>
	internal static DataTable CreateSingleColumn<T>(string columnName, IEnumerable<T?> values)
	{
		DataTable table = new();
		// DataTable no admite Nullable<T> como tipo de columna: hay que usar el tipo subyacente (AllowDBNull ya es
		// true por omisión, así que los nulos se admiten igualmente). "T?" sobre un T sin restricciones no envuelve
		// automáticamente en Nullable<T>, así que T puede llegar aquí siendo ya el propio Nullable<T>
		Type columnType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

			table.Columns.Add(columnName, columnType);
			foreach (T? value in values)
			{
				DataRow row = table.NewRow();

					row[columnName] = value is null ? DBNull.Value : value;
					table.Rows.Add(row);
			}
			return table;
	}

	/// <summary>
	///		Crea una tabla vacía (sin columnas ni filas)
	/// </summary>
	internal static DataTable CreateEmpty() => new();
}
