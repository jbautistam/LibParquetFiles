using FluentAssertions;
using Bau.Libraries.LibParquetFiles.Writers;
using Bau.Libraries.LibParquetFiles.Readers;
using ParquetFiles.Tests.MotherObject;

namespace ParquetFiles.Tests;

/// <summary>
///		Pruebas de <see cref="ParquetDataWriter"/>
/// </summary>
public class ParquetDataWriter_Should
{
	/// <summary>
	///		Comprueba si se puede crear un archivo
	/// </summary>
	[Fact]
	public async Task write_file()
	{
		List<Sale> sales = new DataGenerator().Create(new DateTime(2000, 1, 1), 50_000);
		ParquetDataWriter writer = new(20_000);

			// Escribe los datos
			using (SalesDataReader reader = new(sales))
			{
				await writer.WriteAsync(GetPathFileName("test.parquet"), reader, CancellationToken.None);
			}
			// Comprueba los datos
			using (ParquetDataReader reader = new())
			{
				int row = 0;

					// Abre el archivo
					await reader.OpenAsync(GetPathFileName("test.parquet"), CancellationToken.None);
					// Comprueba los datos
					while (await reader.ReadAsync(CancellationToken.None))
						Check(reader, sales[row++]);
					// Comprueba el número de registros leidos
					row.Should().Be(sales.Count);
			}
	}

	/// <summary>
	///		Comprueba los datos de un registro
	/// </summary>
	private void Check(ParquetDataReader reader, Sale sale)
	{
		reader[reader.GetOrdinal(nameof(sale.Id))].Should().Be(sale.Id.ToString());
		reader[reader.GetOrdinal(nameof(sale.ProductId))].Should().Be(sale.ProductId);
		reader[reader.GetOrdinal(nameof(sale.Date))].Should().Be(sale.Date);
		reader[reader.GetOrdinal(nameof(sale.Quantity))].Should().Be(sale.Quantity);
		reader[reader.GetOrdinal(nameof(sale.Price))].Should().Be(sale.Price);
	}

	/// <summary>
	///		Obtiene el nombre de archivo
	/// </summary>
	private string GetPathFileName(string file) => Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Data", file);
}