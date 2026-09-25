namespace ParquetFiles.Tests.MotherObject;

/// <summary>
///		Genera un nombre de archivo temporal único y lo borra al liberarse
/// </summary>
/// <remarks>
///		Necesario porque xUnit ejecuta las clases de test en paralelo: dos pruebas que escribieran
///	sobre el mismo nombre de archivo fijo entrarían en carrera
/// </remarks>
internal class TempFile : IDisposable
{
	internal TempFile()
	{
		FileName = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.parquet");
	}

	/// <summary>
	///		Libera el archivo temporal (si se llegó a crear)
	/// </summary>
	public void Dispose()
	{
		if (File.Exists(FileName))
			File.Delete(FileName);
	}

	/// <summary>
	///		Ruta del archivo temporal
	/// </summary>
	internal string FileName { get; }
}
