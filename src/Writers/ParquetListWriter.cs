using System;
using System.Collections.Generic;
using System.Data;

using Parquet;
using Parquet.Data.Rows;

namespace Bau.Libraries.LibParquetFiles.Writers
{
	/// <summary>
	///		Clase de escritura sobre archivos Parquet a partir de una serie de <see cref="IDataReader"/>
	/// </summary>
	public class ParquetListWriter : BaseParquetDataWriter, IDisposable
	{
		public ParquetListWriter(string fileName, int notifyAfter = 200_000) : base(fileName, notifyAfter) {}

		/// <summary>
		///		Procesa una exportación de una serie de consultas a un archivo parquet
		/// </summary>
		public long Write(IEnumerable<IDataReader> readers, int rowGroupSize = 45_000)
		{
			long records = 0;

				// Escribe en el archivo
				using (System.IO.FileStream stream = System.IO.File.Open(FileName, System.IO.FileMode.Create, System.IO.FileAccess.Write))
				{
					Parquet.ParquetWriter writer = null;
					Table table = null;

						// Escribe todos los readers
						foreach (IDataReader reader in readers)
						{
							// Ajusta las variables la primera vez
							if (writer == null)
							{
								// Obtiene el esquema
								ReadSchema(reader);
								// Obtiene el objeto de escritura sobre el archivo
								writer = new Parquet.ParquetWriter(Schema, stream);
								// Establece el método de compresión
								//? No asigna el nivel de compresión: deja el predeterminado para el método
								writer.CompressionMethod = CompressionMethod.Snappy;
								// Asigna la tabla
								table = new Table(Schema);
							}
							// Escribe los registros
							records = WriteDataReader(writer, table, reader, rowGroupSize, records);
						}
						// Graba las últimas filas
						FlushRowGroup(writer, table);
				}
				// Devuelve el número de registros escritos
				return records;
		}

		/// <summary>
		///		Escribe los registros de un <see cref="IDataReader"/>
		/// </summary>
		private long WriteDataReader(Parquet.ParquetWriter writer, Table table, IDataReader reader, int rowGroupSize, long actualRecords)
		{
			// Carga los registros y los va añadiendo a la lista para meterlos en un grupo de filas
			while (reader.Read())
			{
				// Escribe la tabla en el archivo si se ha superado el número máximo de filas
				if (table.Count >= rowGroupSize)
				{
					// Escribe la tabla en un grupo de filas
					FlushRowGroup(writer, table);
					// Limpia la tabla
					table.Clear();
				}
				// Añade los datos del registro a la lista
				table.Add(ConvertData(reader));
				// Lanza el evento de progreso
				RaiseProgressEvent(++actualRecords);
			}
			// Devuelve el número de registros
			return actualRecords;
		}
	}
}