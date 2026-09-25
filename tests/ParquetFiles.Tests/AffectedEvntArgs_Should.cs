using FluentAssertions;
using Bau.Libraries.LibParquetFiles.EventArguments;

namespace ParquetFiles.Tests;

/// <summary>
///		Pruebas de <see cref="AffectedEvntArgs"/>
/// </summary>
public class AffectedEvntArgs_Should
{
	/// <summary>
	///		Comprueba que expone el número de registros con el que se construyó
	/// </summary>
	[Fact]
	public void expose_the_records_it_was_built_with()
	{
		AffectedEvntArgs args = new(42);

			args.Records.Should().Be(42);
	}

	/// <summary>
	///		Comprueba que deriva de <see cref="EventArgs"/>, como corresponde al patrón estándar de eventos .NET
	/// </summary>
	[Fact]
	public void derive_from_event_args()
	{
		AffectedEvntArgs args = new(1);

			args.Should().BeAssignableTo<EventArgs>();
	}
}
