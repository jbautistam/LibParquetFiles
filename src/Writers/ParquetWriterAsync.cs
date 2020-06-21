using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Parquet;
using Parquet.Data.Rows;

namespace Bau.Libraries.LibParquetFiles.Writers
{
	/// <summary>
	///		Clase de escritura sobre archivos Parquet leyendo de un <see cref="IDataReader"/>
	/// </summary>
	public class ParquetWriterAsync : BaseParquetDataWriter, IDisposable
	{
		public ParquetWriterAsync(string fileName, int notifyAfter = 200_000) : base(fileName, notifyAfter) {}

		/// <summary>
		///		Procesa una exportación de una consulta a un archivo parquet
		/// </summary>
		public async Task<long> WriteAsync(DbDataReader reader, CancellationToken cancellationToken, int rowGroupSize = 45_000)
		{
			long records = 0;

				// Escribe en el archivo
				using (System.IO.FileStream stream = System.IO.File.Open(FileName, System.IO.FileMode.Create, System.IO.FileAccess.Write))
				{
					// Carga el esquema
					ReadSchema(reader);
					// Escribe los datos
					using (Parquet.ParquetWriter writer = new Parquet.ParquetWriter(Schema, stream))
					{
						Table table = new Table(Schema);

							// Establece el método de compresión
							//? No asigna el nivel de compresión: deja el predeterminado para el método
							writer.CompressionMethod = CompressionMethod.Snappy;
							// Carga los registros y los va añadiendo a la lista para meterlos en un grupo de filas
							while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync(cancellationToken))
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
								RaiseProgressEvent(++records);
							}
							// Graba las últimas filas
							FlushRowGroup(writer, table);
					}
				}
				// Devuelve el número de registros escritos
				return records;
		}
	}
}