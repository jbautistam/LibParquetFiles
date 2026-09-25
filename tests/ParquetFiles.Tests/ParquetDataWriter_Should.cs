using FluentAssertions;
using Bau.Libraries.LibParquetFiles.Writers;
using Bau.Libraries.LibParquetFiles.Readers;
using Parquet;
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
			using (TempFile file = new())
			{
				using (SalesDataReader reader = new(sales))
				{
					await writer.WriteAsync(file.FileName, reader, CancellationToken.None);
				}
				// Comprueba los datos
				using (ParquetDataReader reader = new())
				{
					int row = 0;

						// Abre el archivo
						await reader.OpenAsync(file.FileName, CancellationToken.None);
						// Comprueba los datos
						while (await reader.ReadAsync(CancellationToken.None))
							Check(reader, sales[row++]);
						// Comprueba el número de registros leidos
						row.Should().Be(sales.Count);
				}
			}
	}

	/// <summary>
	///		Comprueba los datos de un registro
	/// </summary>
	private void Check(ParquetDataReader reader, Sale sale)
	{
		reader[reader.GetOrdinal(nameof(sale.Id))].Should().Be(sale.Id);
		reader[reader.GetOrdinal(nameof(sale.ProductId))].Should().Be(sale.ProductId);
		reader[reader.GetOrdinal(nameof(sale.Date))].Should().Be(sale.Date);
		reader[reader.GetOrdinal(nameof(sale.Quantity))].Should().Be(sale.Quantity);
		reader[reader.GetOrdinal(nameof(sale.Price))].Should().Be(sale.Price);
	}

	/// <summary>
	///		Comprueba que se devuelve el número de registros escritos
	/// </summary>
	[Fact]
	public async Task return_the_number_of_written_records()
	{
		using TempFile file = new();
		using FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", Enumerable.Range(0, 7).Cast<int?>());
		ParquetDataWriter writer = new(10);

			long written = await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);

			written.Should().Be(7);
	}

	/// <summary>
	///		Comprueba que se puede escribir directamente sobre un <see cref="Stream"/> en lugar de una ruta
	/// </summary>
	[Fact]
	public async Task write_to_a_stream()
	{
		using TempFile file = new();
		using FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", new int?[] { 1, 2, 3 });
		ParquetDataWriter writer = new(10);

			using (Stream stream = File.Create(file.FileName))
			{
				long written = await writer.WriteAsync(stream, reader, TestContext.Current.CancellationToken);

					written.Should().Be(3);
			}
			using (ParquetDataReader verify = new())
			{
				int count = 0;

					await verify.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
					while (await verify.ReadAsync(TestContext.Current.CancellationToken))
						count++;
					count.Should().Be(3);
			}
	}

	/// <summary>
	///		Comprueba que el evento Progress se lanza cada NotifyAfter registros escritos
	/// </summary>
	[Fact]
	public async Task raise_the_progress_event_every_notify_after_records()
	{
		using TempFile file = new();
		using FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", Enumerable.Range(0, 25).Cast<int?>());
		ParquetDataWriter writer = new(100, notifyAfter: 10);
		List<long> notified = new();

			writer.Progress += (_, e) => notified.Add(e.Records);

			await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);

			notified.Should().Equal(10, 20);
	}

	/// <summary>
	///		Comprueba que un NotifyAfter de cero desactiva el evento Progress (en lugar de lanzar, ver
	///	ParquetDataReader_Should.not_throw_when_notify_after_is_zero para la misma corrección en el lector)
	/// </summary>
	[Fact]
	public async Task not_raise_the_progress_event_when_notify_after_is_zero()
	{
		using TempFile file = new();
		using FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", Enumerable.Range(0, 25).Cast<int?>());
		ParquetDataWriter writer = new(100, notifyAfter: 0);
		bool raised = false;

			writer.Progress += (_, _) => raised = true;

			await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);

			raised.Should().BeFalse();
	}

	/// <summary>
	///		Comprueba que un origen sin filas produce un archivo parquet válido (con esquema, pero sin filas)
	/// </summary>
	[Fact]
	public async Task write_a_valid_empty_file_when_there_are_no_rows()
	{
		using TempFile file = new();
		using FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", Array.Empty<int?>());
		ParquetDataWriter writer = new(10);

			long written = await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);

			written.Should().Be(0);

			using (ParquetDataReader verify = new())
			{
				await verify.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

					(await verify.ReadAsync(TestContext.Current.CancellationToken)).Should().BeFalse();
					verify.FieldCount.Should().Be(1);
			}
	}

	/// <summary>
	///		Comprueba que, si el tamaño del grupo de filas supera el número de filas, se escribe un único grupo
	/// </summary>
	[Fact]
	public async Task write_a_single_row_group_when_the_row_group_size_exceeds_the_rows()
	{
		using TempFile file = new();
		using FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", new int?[] { 1, 2, 3 });
		ParquetDataWriter writer = new(1_000);

			await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);

			using (Stream stream = File.OpenRead(file.FileName))
			await using (ParquetReader parquetReader = await ParquetReader.CreateAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
			{
				parquetReader.RowGroupCount.Should().Be(1);
			}
	}

	/// <summary>
	///		Comprueba que el tamaño del grupo de filas debe ser positivo
	/// </summary>
	[Theory]
	[InlineData(0)]
	[InlineData(-1)]
	public void throw_when_the_row_group_size_is_not_positive(int rowGroupSize)
	{
		Action act = () => new ParquetDataWriter(rowGroupSize);

			act.Should().Throw<ArgumentOutOfRangeException>();
	}

	/// <summary>
	///		Comprueba que escribir sobre una ruta existente la sobrescribe en lugar de añadir datos o fallar
	/// </summary>
	[Fact]
	public async Task overwrite_an_existing_file()
	{
		using TempFile file = new();

			await File.WriteAllTextAsync(file.FileName, "stale content", TestContext.Current.CancellationToken);

			using FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", new int?[] { 1 });
			ParquetDataWriter writer = new(10);

				await writer.WriteAsync(file.FileName, reader, TestContext.Current.CancellationToken);

				using (ParquetDataReader verify = new())
				{
					await verify.OpenAsync(file.FileName, TestContext.Current.CancellationToken);

						(await verify.ReadAsync(TestContext.Current.CancellationToken)).Should().BeTrue();
						((int) verify.GetValue(0)).Should().Be(1);
				}
	}

	/// <summary>
	///		Comprueba que cancelar la escritura lanza en lugar de truncar el archivo en silencio (antes, la
	///	cancelación simplemente cortaba el bucle y devolvía normalmente)
	/// </summary>
	[Fact]
	public async Task throw_when_cancelled()
	{
		using TempFile file = new();
		using CancellationTokenSource cts = new();

			cts.Cancel();

			using FakeDataReader reader = FakeDataReader.CreateSingleColumn("Value", new int?[] { 1, 2, 3 });
			ParquetDataWriter writer = new(10);

				Func<Task> act = () => writer.WriteAsync(file.FileName, reader, cts.Token);

				await act.Should().ThrowAsync<OperationCanceledException>();
	}

	/// <summary>
	///		Comprueba que un origen que declara una columna DateTime/TimeSpan/Guid pero cuyo valor real es una
	///	cadena (o una matriz de 16 bytes para Guid) se convierte correctamente, en lugar de exigir el tipo exacto
	/// </summary>
	[Fact]
	public async Task convert_string_and_byte_array_values_for_date_time_time_span_and_guid_columns()
	{
		(string, Type)[] columns =
			{
				("Date", typeof(DateTime)), ("Time", typeof(TimeSpan)),
				("GuidFromString", typeof(Guid)), ("GuidFromBytes", typeof(Guid))
			};
		Guid guid = Guid.Parse("DC340CF2-331E-4B58-9F96-B5009EAA8987");
		List<object?[]> rows = new() { new object?[] { "2024-05-01T10:30:00", "10:30:00", guid.ToString(), guid.ToByteArray() } };

		using TempFile file = new();

			using (FakeDataReader source = new(columns, rows))
			{
				ParquetDataWriter writer = new(10);

					await writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);
			}
			using (ParquetDataReader reader = new())
			{
				await reader.OpenAsync(file.FileName, TestContext.Current.CancellationToken);
				await reader.ReadAsync(TestContext.Current.CancellationToken);

					reader.GetValue(reader.GetOrdinal("Date")).Should().Be(new DateTime(2024, 5, 1, 10, 30, 0));
					reader.GetValue(reader.GetOrdinal("Time")).Should().Be(new TimeSpan(10, 30, 0));
					reader.GetValue(reader.GetOrdinal("GuidFromString")).Should().Be(guid);
					reader.GetValue(reader.GetOrdinal("GuidFromBytes")).Should().Be(guid);
			}
	}

	/// <summary>
	///		Comprueba que un valor no convertible en una columna TimeSpan lanza en lugar de escribir basura
	/// </summary>
	[Fact]
	public async Task throw_when_a_time_span_column_value_is_not_convertible()
	{
		(string, Type)[] columns = { ("Time", typeof(TimeSpan)) };

		using TempFile file = new();
		using FakeDataReader source = new(columns, new List<object?[]> { new object?[] { 123 } });
		ParquetDataWriter writer = new(10);

			Func<Task> act = () => writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);

			await act.Should().ThrowAsync<InvalidCastException>();
	}

	/// <summary>
	///		Comprueba que un valor no convertible en una columna Guid lanza en lugar de escribir basura
	/// </summary>
	[Fact]
	public async Task throw_when_a_guid_column_value_is_not_convertible()
	{
		(string, Type)[] columns = { ("Id", typeof(Guid)) };

		using TempFile file = new();
		using FakeDataReader source = new(columns, new List<object?[]> { new object?[] { 123 } });
		ParquetDataWriter writer = new(10);

			Func<Task> act = () => writer.WriteAsync(file.FileName, source, TestContext.Current.CancellationToken);

			await act.Should().ThrowAsync<InvalidCastException>();
	}

	/// <summary>
	///		Comprueba que DisposeAsync es idempotente (hoy es un no-op decorativo: WriteAsync no necesita que se
	///	llame a Dispose, pero el contrato de IAsyncDisposable exige que sea seguro llamarlo, incluso varias veces)
	/// </summary>
	[Fact]
	public async Task be_safe_to_dispose_twice()
	{
		ParquetDataWriter writer = new(10);

			await writer.DisposeAsync();
			writer.Disposed.Should().BeTrue();

			Func<Task> act = () => writer.DisposeAsync().AsTask();

			await act.Should().NotThrowAsync();
	}
}
