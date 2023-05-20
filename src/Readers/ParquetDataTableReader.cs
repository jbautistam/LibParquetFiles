using System.Data;

using Parquet;
using Parquet.Schema;

namespace Bau.Libraries.LibParquetFiles.Readers;

/// <summary>
///     Lector de parquet sobre un DataTable
/// </summary>
public class ParquetDataTableReader
{
	/// <summary>
	///		Obtiene un dataTable a partir de un archivo parquet
	/// </summary>
	public async Task<(DataTable table, long totalRecordCount)> ParquetReaderToDataTableAsync(string fileName, int offset, int recordCount, 
																							  CancellationToken cancellationToken)
	{
		DataTable dataTable = new DataTable();
		long totalRecordCount = 0;

			// Lee el archivo
			using (System.IO.Stream fileReader = System.IO.File.OpenRead(fileName))
			{
				using (ParquetReader parquetReader = await ParquetReader.CreateAsync(fileName, cancellationToken: cancellationToken))
				{
					DataField[] dataFields = parquetReader.Schema.GetDataFields();

						// Crea las columnas en la tabla
						CreateColumns(dataTable, dataFields);
						//Read column by column to generate each row in the datatable
						for (int rowGroup = 0; rowGroup < parquetReader.RowGroupCount; rowGroup++)
						{
							long rowsLeftToRead = recordCount;

							using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(rowGroup))
							{
								if (groupReader.RowCount > int.MaxValue)
									throw new ArgumentOutOfRangeException(string.Format("Cannot handle row group sizes greater than {0}", groupReader.RowCount));

								long rowsPassedUntilThisRowGroup = totalRecordCount;
								totalRecordCount += (int) groupReader.RowCount;

								if (offset >= totalRecordCount)
									continue;

								if (rowsLeftToRead > 0)
								{
									long numberOfRecordsToReadFromThisRowGroup = Math.Min(Math.Min(totalRecordCount - offset, recordCount), (int) groupReader.RowCount);
									rowsLeftToRead -= numberOfRecordsToReadFromThisRowGroup;

									long recordsToSkipInThisRowGroup = Math.Max(offset - rowsPassedUntilThisRowGroup, 0);

									await ProcessRowGroupAsync(dataTable, groupReader, dataFields, recordsToSkipInThisRowGroup, numberOfRecordsToReadFromThisRowGroup,
															   cancellationToken);
								}
							}
						}
				}
			}
			// Devuelve los datos leidos
			return (dataTable, totalRecordCount);
	}

	/// <summary>
	///		Crea las columnas de la tabla
	/// </summary>
	private void CreateColumns(DataTable dataTable, DataField[] fields)
	{
            foreach (DataField field in fields)
                dataTable.Columns.Add(new DataColumn(field.Name, field.ClrType));
	}

	/// <summary>
	///		Procesa un grupo de filas
	/// </summary>
	private async Task ProcessRowGroupAsync(DataTable dataTable, ParquetRowGroupReader groupReader, DataField[] fields, long skipRecords, long readRecords,
											CancellationToken cancellationToken)
	{
		int rowBeginIndex = dataTable.Rows.Count;
		bool isFirstColumn = true;

			foreach (DataField field in fields)
			{
				int rowIndex = rowBeginIndex;
				int skippedRecords = 0;

					foreach (object value in (await groupReader.ReadColumnAsync(field, cancellationToken)).Data)
					{
						// Se salta los primeros registros
						if (skipRecords > skippedRecords)
						{
							skippedRecords++;
							continue;
						}
						// Si ha pasado el número de registros a leer, sale del bucle
						if (rowIndex >= readRecords)
							break;
						// Si es la primera columna, crea una nueva fila
						if (isFirstColumn)
						{
							DataRow newRow = dataTable.NewRow();

								// Añade la fila
								dataTable.Rows.Add(newRow);
						}
						// Asigna el valor a la columna
						if (value is null)
							dataTable.Rows[rowIndex][field.Name] = DBNull.Value;
						else
							dataTable.Rows[rowIndex][field.Name] = value;
						// Incrementa el índice de la fila
						rowIndex++;
					}
					// Indica que no es la primera columna
					isFirstColumn = false;
			}
	}
}
