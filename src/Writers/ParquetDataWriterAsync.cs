﻿using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Bau.Libraries.LibParquetFiles.Writers
{
	/// <summary>
	///		Clase de escritura sobre archivos Parquet leyendo de un <see cref="IDataReader"/>
	/// </summary>
	public class ParquetDataWriterAsync : IAsyncDisposable
	{
		// Eventos públicos
		public event EventHandler<EventArguments.AffectedEvntArgs> Progress;

		public ParquetDataWriterAsync(int rowGroupSize, int notifyAfter = 200_000)
		{
			RowGroupSize = rowGroupSize;
			NotifyAfter = notifyAfter;
		}

		/// <summary>
		///		Lanza el evento de progreso
		/// </summary>
		private void RaiseProgressEvent(long records)
		{
			if (NotifyAfter > 0 && records % NotifyAfter == 0)
				Progress?.Invoke(this, new EventArguments.AffectedEvntArgs(records));
		}

		/// <summary>
		///		Procesa una exportación de una consulta a un archivo parquet
		/// </summary>
		public async Task<long> WriteAsync(string fileName, IDataReader reader, CancellationToken cancellationToken)
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
		public async Task<long> WriteAsync(System.IO.Stream stream, IDataReader reader, CancellationToken cancellationToken)
		{
			long records = 0;

				// Escribe los datos
				await using (Models.ParquetFileModel file = new Models.ParquetFileModel(RowGroupSize))
				{
					// Abre el archivo
					await file.OpenAsync(stream, reader, cancellationToken);
					// Carga los registros y los va añadiendo a la lista para meterlos en un grupo de filas
					while (!cancellationToken.IsCancellationRequested && reader.Read())
					{
						// Lee el registro (graba el grupo si ha sobrepasado el número de registros en la caché)
						await file.ReadRecordAsync(reader, cancellationToken);
						// Lanza el evento de progreso
						RaiseProgressEvent(++records);
					}
					// Graba las últimas filas
					await file.FlushAsync(cancellationToken);
				}
				// Devuelve el número de registros escritos
				return records;
		}


		/// <summary>
		///		Libera la memoria
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			if (!Disposed)
			{
				// Evita las advertencias
				await Task.Delay(1);
				// Indica que se ha liberado
				Disposed = true;
			}
		}

		/// <summary>
		///		Tamaño del grupo de filas
		/// </summary>
		public int RowGroupSize { get; }

		/// <summary>
		///		Número de registros después de los que se debe notificar
		/// </summary>
		public int NotifyAfter { get; }

		/// <summary>
		///		Indica si se ha liberado la memoria
		/// </summary>
		public bool Disposed { get; private set; }
	}
}