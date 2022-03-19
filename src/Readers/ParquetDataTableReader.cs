using System;
using System.Data;

using Parquet;
using Parquet.Data;

namespace Bau.Libraries.LibParquetFiles.Readers
{
	/// <summary>
	///     Lector de parquet sobre un DataTable
	/// </summary>
	public class ParquetDataTableReader
	{
		/// <summary>
		///		Obtiene un dataTable a partir de un archivo parquet
		/// </summary>
		public DataTable ParquetReaderToDataTable(string fileName, int offset, int recordCount, out long totalRecordCount)
		{
			DataTable dataTable = new DataTable();

				// Inicializa el número total de registros
				totalRecordCount = 0;
				// Lee el archivo
				using (System.IO.Stream fileReader = System.IO.File.OpenRead(fileName))
				{
					using (ParquetReader parquetReader = new ParquetReader(fileReader))
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

										ProcessRowGroup(dataTable, groupReader, dataFields, recordsToSkipInThisRowGroup, numberOfRecordsToReadFromThisRowGroup);
									}
								}
							}
					}
				}
				// Devuelve los datos leidos
				return dataTable;
		}

		/// <summary>
		///		Crea las columnas de la tabla
		/// </summary>
		private void CreateColumns(DataTable dataTable, DataField[] fields)
		{
            foreach (DataField field in fields)
                dataTable.Columns.Add(new System.Data.DataColumn(field.Name, Convert(field.DataType)));
		}

		/// <summary>
		///		Procesa un grupo de filas
		/// </summary>
		private void ProcessRowGroup(DataTable dataTable, ParquetRowGroupReader groupReader, DataField[] fields, long skipRecords, long readRecords)
		{
			int rowBeginIndex = dataTable.Rows.Count;
			bool isFirstColumn = true;

			foreach (DataField field in fields)
			{
				int rowIndex = rowBeginIndex;

				int skippedRecords = 0;
				foreach (object value in groupReader.ReadColumn(field).Data)
				{
					if (skipRecords > skippedRecords)
					{
						skippedRecords++;
						continue;
					}

					if (rowIndex >= readRecords)
						break;

					if (isFirstColumn)
					{
						DataRow newRow = dataTable.NewRow();
						dataTable.Rows.Add(newRow);
					}

					if (value == null)
						dataTable.Rows[rowIndex][field.Name] = DBNull.Value;
					else if (field.DataType == DataType.DateTimeOffset) // Convierte la fecha a la hora local
						dataTable.Rows[rowIndex][field.Name] = ((DateTimeOffset) value).DateTime;
					else
						dataTable.Rows[rowIndex][field.Name] = value;

					rowIndex++;
				}

				isFirstColumn = false;
			}
		}

		/// <summary>
		///		Convierte el tipo de datos
		/// </summary>
		private Type Convert(DataType type)
		{
			switch (type)
			{
				case DataType.Boolean:
					return typeof(bool);
				case DataType.Byte:
					return typeof(sbyte);
				case DataType.ByteArray:
					return typeof(sbyte[]);
				case DataType.DateTimeOffset: // tratamos dateTimeOffsets como dateTime
					return typeof(DateTime);
				case DataType.Decimal:
					return typeof(decimal);
				case DataType.Double:
					return typeof(double);
				case DataType.Float:
					return typeof(float);
				case DataType.Short:
				case DataType.Int16:
				case DataType.Int32:
				case DataType.UnsignedInt16:
					return typeof(int);
				case DataType.Int64:
					return typeof(long);
				case DataType.UnsignedByte:
					return typeof(byte);
				default:
					return typeof(string);
			}
		}
	}
}
