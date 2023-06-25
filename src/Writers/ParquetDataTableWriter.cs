using System.Data;

namespace Bau.Libraries.LibParquetFiles.Writers;

/// <summary>
///		Clase de escritura sobre archivos Parquet leyendo de una serie de <see cref="DataTable"/>
/// </summary>
public class ParquetDataTableWriter : IAsyncDisposable
{
	// Variables privadas
	private FileStream? _stream;
	private Models.ParquetFileModel? _file;
	private bool _writtenSchema = false;

	public ParquetDataTableWriter(int rowGroupSize)
	{
		RowGroupSize = rowGroupSize;
	}

	/// <summary>
	///		Abre el archivo
	/// </summary>
	public void Open(string fileName)
	{
		_stream = File.Open(fileName, FileMode.Create, FileAccess.Write);
		_file = new Models.ParquetFileModel(RowGroupSize);
	}

	/// <summary>
	///		Procesa una exportación de un <see cref="DataTable"/> a un archivo parquet
	/// </summary>
	public async Task WriteAsync(DataTable table, CancellationToken cancellationToken)
	{
		if (_file is null || _stream is null)
			throw new NotImplementedException("File is closed");
		else
		{
			// Lee los datos
			using (IDataReader reader = table.CreateDataReader())
			{
				// Crea el esquema si no se había creado
				if (!_writtenSchema)
				{
					await _file.OpenAsync(_stream, reader, cancellationToken);
					_writtenSchema = true;
				}
				// Carga los registros y los va añadiendo a la lista para meterlos en un grupo de filas
				while (!cancellationToken.IsCancellationRequested && reader.Read())
					await _file.WriteRecordAsync(reader, cancellationToken);
			}
		}
	}

	/// <summary>
	///		Graba las últimas filas del archivo
	/// </summary>
	public async Task FlushAsync(CancellationToken cancellationToken)
	{
		if (_file is null || _stream is null)
			throw new NotImplementedException("File is closed");
		else
			await _file.FlushAsync(cancellationToken);
	}

	/// <summary>
	///		Libera la memoria
	/// </summary>
	public async ValueTask DisposeAsync()
	{
		if (!Disposed)
		{
			// Elimina los datos
			if (_file is not null)
				await _file.DisposeAsync();
			if (_stream is not null)
			{
				await _stream.FlushAsync();
				_stream.Close();
				_stream = null;
			}
			// Indica que se ha liberado
			Disposed = true;
		}
	}

	/// <summary>
	///		Tamaño del grupo de filas
	/// </summary>
	public int RowGroupSize { get; }

	/// <summary>
	///		Indica si se ha liberado la memoria
	/// </summary>
	public bool Disposed { get; private set; }
}