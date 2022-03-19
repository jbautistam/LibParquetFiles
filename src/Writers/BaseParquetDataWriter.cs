using System;

namespace Bau.Libraries.LibParquetFiles.Writers
{
	/// <summary>
	///		Base para un generador de archivos Parquet
	/// </summary>
	public abstract class BaseParquetDataWriter : IDisposable
	{
		// Eventos públicos
		public event EventHandler<EventArguments.AffectedEvntArgs> Progress;

		protected BaseParquetDataWriter(int rowGroupSize, int notifyAfter = 200_000)
		{
			RowGroupSize = rowGroupSize;
			NotifyAfter = notifyAfter;
		}

		/// <summary>
		///		Lanza el evento de progreso
		/// </summary>
		protected void RaiseProgressEvent(long records)
		{
			if (NotifyAfter > 0 && records % NotifyAfter == 0)
				Progress?.Invoke(this, new EventArguments.AffectedEvntArgs(records));
		}

		/// <summary>
		///		Libera la memoria
		/// </summary>
		protected virtual void Dispose(bool disposing)
		{
			if (!Disposed)
				Disposed = true;
		}

		/// <summary>
		///		Libera la memoria
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
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