using System;
using System.Data;

namespace Bau.Libraries.LibParquetFiles.Writers
{
	/// <summary>
	///		Clase de escritura sobre archivos Parquet
	/// </summary>
	public class ParquetWriter : BaseParquetDataWriter
	{
		public ParquetWriter(int rowGroupSize, int notifyAfter = 200_000) : base(rowGroupSize, notifyAfter) {}

		/// <summary>
		///		Procesa una exportación de una consulta a un archivo parquet
		/// </summary>
		public long Write(string fileName, IDataReader reader)
		{
			long records = 0;

				// Escribe en el archivo
				using (System.IO.FileStream stream = System.IO.File.Open(fileName, System.IO.FileMode.Create, System.IO.FileAccess.Write))
				{
					records = Write(stream, reader);
				}
				// Devuelve el número de registros escritos
				return records;
		}

		/// <summary>
		///		Procesa una exportación de una consulta a un archivo parquet
		/// </summary>
		public long Write(System.IO.Stream stream, IDataReader reader)
		{
			long records = 0;

				// Escribe los datos en el archivo parquet
				using (Models.ParquetFileModel file = new Models.ParquetFileModel(RowGroupSize))
				{
					// Abre el archivo
					file.Open(stream, reader);
					// Carga los registros y los va añadiendo a la lista para meterlos en un grupo de filas
					while (reader.Read())
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