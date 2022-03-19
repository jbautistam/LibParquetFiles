using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Bau.Libraries.LibParquetFiles.Writers
{
	/// <summary>
	///		Clase de escritura sobre archivos Parquet leyendo de un <see cref="IDataReader"/>
	/// </summary>
	public class ParquetWriterAsync : BaseParquetDataWriter
	{
		public ParquetWriterAsync(int rowGroupSize, int notifyAfter = 200_000) : base(rowGroupSize, notifyAfter) {}

		/// <summary>
		///		Procesa una exportación de una consulta a un archivo parquet
		/// </summary>
		public async Task<long> WriteAsync(string fileName, DbDataReader reader, CancellationToken cancellationToken)
		{
			long records = 0;

				// Escribe en el archivo
				using (System.IO.FileStream stream = System.IO.File.Open(fileName, System.IO.FileMode.Create, System.IO.FileAccess.Write))
				{
					records = await WriteAsync(stream, reader, cancellationToken);
				}
				// Devuelve el número de registros escritos
				return records;
		}

		/// <summary>
		///		Procesa una exportación de una consulta a un archivo parquet
		/// </summary>
		public async Task<long> WriteAsync(System.IO.Stream stream, DbDataReader reader, CancellationToken cancellationToken)
		{
			long records = 0;

				// Escribe los datos
				using (Models.ParquetFileModel file = new Models.ParquetFileModel(RowGroupSize))
				{
					// Abre el archivo
					file.Open(stream, reader);
					// Carga los registros y los va añadiendo a la lista para meterlos en un grupo de filas
					while (!cancellationToken.IsCancellationRequested && await reader.ReadAsync(cancellationToken))
					{
						// Lee el registro (graba el grupo si ha sobrepasado el número de registros en la caché)
						file.ReadRecord(reader);
						// Lanza el evento de progreso
						RaiseProgressEvent(++records);
					}
					// Graba las últimas filas
					file.Flush();
				}
				// Devuelve el número de registros escritos
				return records;
		}
	}
}