using CommandLine;
using LibEsri;
using LibGoogleEarth;
using LibMapCommon;
using LibMapCommon.Geometry;
using OSGeo.OGR;
using System.Text;
using System.Text.Json;

namespace GEHistoricalImagery.Cli;

[Verb("availability", HelpText = "Get imagery date availability in a specified region")]
internal class Availability : AoiVerb
{
	[Option('p', "parallel", HelpText = "Number of concurrent downloads", MetaValue = "N", Default = 20)]
	public int ConcurrentDownload { get; set; }

	[Option('j', "json", HelpText = "Output GeoJSON to console")]
	public bool OutputJson { get; set; }

	public override async Task RunAsync()
	{
		bool hasError = false;

		foreach (var errorMessage in GetAoiErrors())
		{
			Console.Error.WriteLine(errorMessage);
			hasError = true;
		}

		if (hasError) return;
		Console.OutputEncoding = Encoding.Unicode;

		await (Provider is Provider.Wayback ? Run_Esri() : Run_Keyhole());
	}

	#region Esri
	private async Task Run_Esri()
	{
		if (ConcurrentDownload > 10)
		{
			ConcurrentDownload = 10;
			Console.Error.WriteLine($"Limiting to {ConcurrentDownload} concurrent scrapes of Esri metadata.");
		}

		var wayBack = await WayBack.CreateAsync(CacheDir);

		var all = await GetAllEsriRegions(wayBack, Region);

		if (all.Sum(r => r.Availabilities.Length) == 0)
		{
			Console.Error.WriteLine($"No imagery available at zoom level {ZoomLevel}");
			return;
		}

		if (OutputJson)
		{
			var mercAoi = Region.ToWebMercator();
			var stats = mercAoi.GetPolygonalRegionStats<EsriTile>(ZoomLevel);
			var allAvailabilities = all.SelectMany(r => r.Availabilities).ToArray();

			WriteGeoJsonEsri(allAvailabilities, mercAoi, stats);
		}
		else
		{
			OptionChooser<EsriRegion>.WaitForOptions(all);
		}
	}

	private async Task<EsriRegion[]> GetAllEsriRegions(WayBack wayBack, GeoRegion<Wgs1984> aoi)
	{
		var mercAoi = aoi.ToWebMercator();
		var stats = mercAoi.GetPolygonalRegionStats<EsriTile>(ZoomLevel);

		ParallelProcessor<EsriRegion> processor = new(ConcurrentDownload);
		List<EsriRegion> allLayers = new();

		await foreach (var region in processor.EnumerateResults(wayBack.Layers.Select(getLayerDates)))
		{
			allLayers.Add(region);
		}

		//De-duplicate list
		allLayers.Sort((a, b) => a.Layer.Date.CompareTo(b.Layer.Date));

		for (int i = 1; i < allLayers.Count; i++)
		{
			for (int k = i - 1; k >= 0; k--)
			{
				if (allLayers[i].Availabilities.SequenceEqual(allLayers[k].Availabilities))
				{
					allLayers.RemoveAt(i--);
					break;
				}
			}
		}

		return allLayers.OrderByDescending(l => l.Date).ToArray();

		async Task<EsriRegion> getLayerDates(LibEsri.Layer layer)
		{
			var regions = await wayBack.GetDateRegionsAsync(layer, mercAoi, ZoomLevel);

			List<RegionAvailability> displays = new(regions.Length);

			for (int i = 0; i < regions.Length; i++)
			{
				var availability = new RegionAvailability(regions[i].Date, stats.NumRows, stats.NumColumns);

				foreach (var tile in mercAoi.GetTiles<EsriTile>(ZoomLevel))
				{
					var cIndex = LibMapCommon.Util.Mod(tile.Column - stats.MinColumn, 1 << tile.Level);
					var rIndex = tile.Row - stats.MinRow;

					availability[rIndex, cIndex] = regions[i].ContainsTile(tile);
				}

				if (availability.HasAnyTiles())
					displays.Add(availability);
			}

			return new EsriRegion(layer, displays.OrderByDescending(d => d.Date).ToArray());
		}
	}

	private class EsriRegion(LibEsri.Layer layer, RegionAvailability[] regions) : IConsoleOption
	{
		public LibEsri.Layer Layer { get; } = layer;
		public RegionAvailability[] Availabilities { get; } = regions;

		public DateOnly Date => Layer.Date;
		public string DisplayValue => DateString(Date);

		public bool DrawOption()
		{
			if (Availabilities.Length == 1)
			{
				var availabilityStr = $"Tile availability on {DateString(Layer.Date)} (captured on {DateString(Availabilities[0].Date)})";
				Console.WriteLine("\r\n" + availabilityStr);
				Console.WriteLine(new string('=', availabilityStr.Length) + "\r\n");

				Availabilities[0].DrawMap();
			}
			else if (Availabilities.Length > 1)
			{
				var availabilityStr = $"Layer {Layer.Title} has imagery from {Availabilities.Length} different dates";
				Console.WriteLine("\r\n" + availabilityStr);
				Console.WriteLine(new string('=', availabilityStr.Length) + "\r\n");

				OptionChooser<RegionAvailability>.WaitForOptions(Availabilities);
			}
			return false;
		}
	}

	#endregion

	#region Keyhole
	private async Task Run_Keyhole()
	{
		var root = await DbRoot.CreateAsync(Database.TimeMachine, CacheDir);

		var all = await GetAllDatesAsync(root, Region);

		if (all.Length == 0)
		{
			Console.Error.WriteLine($"No dated imagery available at zoom level {ZoomLevel}");
			return;
		}

		if (OutputJson)
		{
			var stats = Region.GetPolygonalRegionStats<KeyholeTile>(ZoomLevel);

			WriteGeoJsonKeyhole(all, Region, stats);
		}
		else
		{
			OptionChooser<RegionAvailability>.WaitForOptions(all);
		}
	}

	private async Task<RegionAvailability[]> GetAllDatesAsync(DbRoot root, GeoRegion<Wgs1984> reg)
	{
		var stats = reg.GetPolygonalRegionStats<KeyholeTile>(ZoomLevel);

		ParallelProcessor<List<DatedTile>> processor = new(ConcurrentDownload);

		Dictionary<DateOnly, RegionAvailability> uniqueDates = new();
		HashSet<Tuple<int, int>> uniquePoints = new();

		await foreach (var dSet in processor.EnumerateResults(reg.GetTiles<KeyholeTile>(ZoomLevel).Select(getDatedTiles)))
		{
			foreach (var d in dSet)
			{
				if (!uniqueDates.TryGetValue(d.Date, out RegionAvailability? region))
				{
					region = new RegionAvailability(d.Date, stats.NumRows, stats.NumColumns);
					uniqueDates.Add(d.Date, region);
				}

				var cIndex = LibMapCommon.Util.Mod(d.Tile.Column - stats.MinColumn, 1 << d.Tile.Level);
				var rIndex = stats.MaxRow - d.Tile.Row;

				uniquePoints.Add(new Tuple<int, int>(rIndex, cIndex));
				region[rIndex, cIndex] = await root.GetNodeAsync(d.Tile) is not null;
			}
		}

		//Go back and mark unavailable tiles within the region of interest
		foreach (var a in uniqueDates.Values)
		{
			for (int r = 0; r < a.Height; r++)
			{
				for (int c = 0; c < a.Width; c++)
				{
					if (uniquePoints.Contains(new Tuple<int, int>(r, c)) && a[r, c] is null)
						a[r, c] = false;
				}
			}
		}

		return uniqueDates.Values.OrderByDescending(r => r.Date).ToArray();

		async Task<List<DatedTile>> getDatedTiles(KeyholeTile tile)
		{
			List<DatedTile> dates = new();

			if (await root.GetNodeAsync(tile) is not TileNode node)
				return dates;

			foreach (var datedTile in node.GetAllDatedTiles())
			{
				if (datedTile.Date.Year == 1) continue;

				if (!dates.Any(d => d.Date == datedTile.Date))
					dates.Add(datedTile);
			}
			return dates;
		}
	}

	#endregion

	#region Common

	private class RegionAvailability : IEquatable<RegionAvailability>, IConsoleOption
	{
		public DateOnly Date { get; }
		public string DisplayValue => DateString(Date);
		private bool?[,] Availability { get; }

		public int Height => Availability.GetLength(0);
		public int Width => Availability.GetLength(1);
		public bool? this[int rIndex, int cIndex]
		{
			get => Availability[rIndex, cIndex];
			set => Availability[rIndex, cIndex] = value;
		}

		public RegionAvailability(DateOnly date, int height, int width)
		{
			Date = date;
			Availability = new bool?[height, width];
		}

		public bool HasAnyTiles() => Availability.OfType<bool>().Any(b => b);
		
		public static bool operator ==(RegionAvailability a, RegionAvailability b)=> a.Equals(b);
		public static bool operator !=(RegionAvailability a, RegionAvailability b)=> !a.Equals(b);
		public override int GetHashCode() => HashCode.Combine(Date, Availability);
		public override bool Equals(object? obj) => Equals(obj as RegionAvailability);
		public bool Equals(RegionAvailability? other)
		{
			if (other == null || other.Date != Date || other.Height != Height || other.Width != Width)
				return false;

			for (int i = 0; i < Height; i++)
			{
				for (int j = 0; j < Width; j++)
				{
					if (other.Availability[i, j] != Availability[i, j])
						return false;
				}
			}
			return true;
		}

		public bool DrawOption()
		{
			var availabilityStr = $"Tile availability on {DateString(Date)}";
			Console.WriteLine("\r\n" + availabilityStr);
			Console.WriteLine(new string('=', availabilityStr.Length) + "\r\n");
			DrawMap();
			return false;
		}

		public void DrawMap()
		{
			/*
			 _________________________
			 | Top       | TTTFFFNNN |
			 ------------|------------
			 | Bottom    | TFNTFNTFN |
			 ------------|------------
			 | Character | █▀▀▄:˙▄.  |
			 -------------------------
			 */

			for (int y = 0; y < Height; y += 2)
			{
				var has2Rows = y + 1 < Height;
				char[] row = new char[Width];
				for (int x = 0; x < Width; x++)
				{
					var top = Availability[y, x];
					if (has2Rows)
					{
						var bottom = Availability[y + 1, x];
						row[x] = top is true & bottom is true ? '█' :
							top is true ? '▀' :
							bottom is true ? '▄' :
							top is false & bottom is false ? ':' :
							top is false ? '˙' :
							bottom is false ? '.' : ' ';
					}
					else
					{
						row[x] = top is true ? '▀' :
							top is false ? '˙' : ' ';
					}
				}

				Console.WriteLine(new string(row));
			}
		}
	}

	private void WriteGeoJsonKeyhole(RegionAvailability[] availabilities, GeoRegion<Wgs1984> region, TileStats stats)
	{
		using var stream = Console.OpenStandardOutput();
		using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true });

		writer.WriteStartObject();
		writer.WriteString("type", "FeatureCollection");
		writer.WriteStartArray("features");

		foreach (var availability in availabilities)
		{
			// Collect all available tiles for this date
			var multiPolygon = new Geometry(wkbGeometryType.wkbMultiPolygon);

			for (int r = 0; r < availability.Height; r++)
			{
				for (int c = 0; c < availability.Width; c++)
				{
					var isAvailable = availability[r, c];
					if (isAvailable == null || isAvailable == false)
						continue;

					var tileRow = stats.MaxRow - r;
					var tileColumn = (c + stats.MinColumn) % (1 << ZoomLevel);
					var tile = KeyholeTile.Create(tileRow, tileColumn, ZoomLevel);

					// Create polygon for this tile and add to collection
					using var polygon = CreateTilePolygon(tile);
					multiPolygon.AddGeometry(polygon);
				}
			}

			// Merge all adjacent polygons using OGR's Union operation
			if (multiPolygon.GetGeometryCount() > 0)
			{
				using var merged = multiPolygon.UnionCascaded();
				WriteOgrGeometryToGeoJson(writer, merged, availability.Date);
			}

			multiPolygon.Dispose();
		}

		writer.WriteEndArray();
		writer.WriteEndObject();
	}

	private void WriteGeoJsonEsri(RegionAvailability[] availabilities, GeoRegion<WebMercator> region, TileStats stats)
	{
		using var stream = Console.OpenStandardOutput();
		using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true });

		writer.WriteStartObject();
		writer.WriteString("type", "FeatureCollection");
		writer.WriteStartArray("features");

		foreach (var availability in availabilities)
		{
			// Collect all available tiles for this date
			var multiPolygon = new Geometry(wkbGeometryType.wkbMultiPolygon);

			for (int r = 0; r < availability.Height; r++)
			{
				for (int c = 0; c < availability.Width; c++)
				{
					var isAvailable = availability[r, c];
					if (isAvailable == null || isAvailable == false)
						continue;

					var tileRow = r + stats.MinRow;
					var tileColumn = (c + stats.MinColumn) % (1 << ZoomLevel);
					var tile = EsriTile.Create(tileRow, tileColumn, ZoomLevel);

					// Create polygon for this tile and add to collection
					using var polygon = CreateTilePolygon(tile);
					multiPolygon.AddGeometry(polygon);
				}
			}

			// Merge all adjacent polygons using OGR's Union operation
			if (multiPolygon.GetGeometryCount() > 0)
			{
				using var merged = multiPolygon.UnionCascaded();
				WriteOgrGeometryToGeoJson(writer, merged, availability.Date);
			}

			multiPolygon.Dispose();
		}

		writer.WriteEndArray();
		writer.WriteEndObject();
	}

	private static void WriteTileFeature(Utf8JsonWriter writer, ITile<Wgs1984> tile, DateOnly date, bool isAvailable)
	{
		writer.WriteStartObject(); // Feature
		writer.WriteString("type", "Feature");

		// Geometry
		writer.WriteStartObject("geometry");
		writer.WriteString("type", "Polygon");
		writer.WriteStartArray("coordinates");
		writer.WriteStartArray(); // Outer ring

		// Write coordinates as [longitude, latitude]
		WriteCoordinate(writer, tile.LowerLeft);
		WriteCoordinate(writer, tile.UpperLeft);
		WriteCoordinate(writer, tile.UpperRight);
		WriteCoordinate(writer, tile.LowerRight);
		WriteCoordinate(writer, tile.LowerLeft); // Close the ring

		writer.WriteEndArray(); // End outer ring
		writer.WriteEndArray(); // End coordinates array
		writer.WriteEndObject(); // End geometry

		// Properties
		writer.WriteStartObject("properties");
		writer.WriteString("date", date.ToString("yyyy-MM-dd"));
		writer.WriteBoolean("available", isAvailable);
		writer.WriteNumber("row", tile.Row);
		writer.WriteNumber("column", tile.Column);
		writer.WriteNumber("level", tile.Level);
		writer.WriteEndObject(); // End properties

		writer.WriteEndObject(); // End feature
	}

	private static void WriteTileFeature(Utf8JsonWriter writer, ITile<WebMercator> tile, DateOnly date, bool isAvailable)
	{
		writer.WriteStartObject(); // Feature
		writer.WriteString("type", "Feature");

		// Geometry
		writer.WriteStartObject("geometry");
		writer.WriteString("type", "Polygon");
		writer.WriteStartArray("coordinates");
		writer.WriteStartArray(); // Outer ring

		// Write coordinates as [longitude, latitude] - convert WebMercator to WGS84
		WriteCoordinate(writer, tile.LowerLeft.ToWgs1984());
		WriteCoordinate(writer, tile.UpperLeft.ToWgs1984());
		WriteCoordinate(writer, tile.UpperRight.ToWgs1984());
		WriteCoordinate(writer, tile.LowerRight.ToWgs1984());
		WriteCoordinate(writer, tile.LowerLeft.ToWgs1984()); // Close the ring

		writer.WriteEndArray(); // End outer ring
		writer.WriteEndArray(); // End coordinates array
		writer.WriteEndObject(); // End geometry

		// Properties
		writer.WriteStartObject("properties");
		writer.WriteString("date", date.ToString("yyyy-MM-dd"));
		writer.WriteNumber("level", tile.Level);
		writer.WriteEndObject(); // End properties

		writer.WriteEndObject(); // End feature
	}

	private static void WriteCoordinate(Utf8JsonWriter writer, Wgs1984 coordinate)
	{
		writer.WriteStartArray();
		writer.WriteNumberValue(coordinate.Longitude);
		writer.WriteNumberValue(coordinate.Latitude);
		writer.WriteEndArray();
	}

	/// <summary>
	/// Creates an OGR Polygon geometry from a tile's corners
	/// </summary>
	private static Geometry CreateTilePolygon(ITile<Wgs1984> tile)
	{
		var ring = new Geometry(wkbGeometryType.wkbLinearRing);

		// Add points in counter-clockwise order (GeoJSON standard)
		ring.AddPoint_2D(tile.LowerLeft.Longitude, tile.LowerLeft.Latitude);
		ring.AddPoint_2D(tile.UpperLeft.Longitude, tile.UpperLeft.Latitude);
		ring.AddPoint_2D(tile.UpperRight.Longitude, tile.UpperRight.Latitude);
		ring.AddPoint_2D(tile.LowerRight.Longitude, tile.LowerRight.Latitude);
		ring.AddPoint_2D(tile.LowerLeft.Longitude, tile.LowerLeft.Latitude); // Close the ring

		var polygon = new Geometry(wkbGeometryType.wkbPolygon);
		polygon.AddGeometry(ring);

		return polygon;
	}

	/// <summary>
	/// Creates an OGR Polygon geometry from a WebMercator tile (converts to WGS84)
	/// </summary>
	private static Geometry CreateTilePolygon(ITile<WebMercator> tile)
	{
		var ring = new Geometry(wkbGeometryType.wkbLinearRing);

		// Convert to WGS84 and add points in counter-clockwise order
		var ll = tile.LowerLeft.ToWgs1984();
		var ul = tile.UpperLeft.ToWgs1984();
		var ur = tile.UpperRight.ToWgs1984();
		var lr = tile.LowerRight.ToWgs1984();

		ring.AddPoint_2D(ll.Longitude, ll.Latitude);
		ring.AddPoint_2D(ul.Longitude, ul.Latitude);
		ring.AddPoint_2D(ur.Longitude, ur.Latitude);
		ring.AddPoint_2D(lr.Longitude, lr.Latitude);
		ring.AddPoint_2D(ll.Longitude, ll.Latitude); // Close the ring

		var polygon = new Geometry(wkbGeometryType.wkbPolygon);
		polygon.AddGeometry(ring);

		return polygon;
	}

	/// <summary>
	/// Writes an OGR Geometry (Polygon or MultiPolygon) to GeoJSON with date property
	/// </summary>
	private static void WriteOgrGeometryToGeoJson(Utf8JsonWriter writer, Geometry geometry, DateOnly date)
	{
		var geomType = geometry.GetGeometryType();

		if (geomType == wkbGeometryType.wkbPolygon)
		{
			WritePolygonFeature(writer, geometry, date);
		}
		else if (geomType == wkbGeometryType.wkbMultiPolygon)
		{
			// Write each polygon as a separate feature
			int count = geometry.GetGeometryCount();
			for (int i = 0; i < count; i++)
			{
				using var polygon = geometry.GetGeometryRef(i);
				WritePolygonFeature(writer, polygon, date);
			}
		}
	}

	private static void WritePolygonFeature(Utf8JsonWriter writer, Geometry polygon, DateOnly date)
	{
		writer.WriteStartObject(); // Feature
		writer.WriteString("type", "Feature");

		// Geometry
		writer.WriteStartObject("geometry");
		writer.WriteString("type", "Polygon");
		writer.WriteStartArray("coordinates");

		// Outer ring (index 0)
		if (polygon.GetGeometryCount() > 0)
		{
			using var ring = polygon.GetGeometryRef(0);
			writer.WriteStartArray();

			int pointCount = ring.GetPointCount();
			for (int i = 0; i < pointCount; i++)
			{
				double[] point = new double[3];
				ring.GetPoint(i, point);

				writer.WriteStartArray();
				writer.WriteNumberValue(point[0]); // Longitude
				writer.WriteNumberValue(point[1]); // Latitude
				writer.WriteEndArray();
			}

			writer.WriteEndArray();
		}

		writer.WriteEndArray(); // End coordinates
		writer.WriteEndObject(); // End geometry

		// Properties
		writer.WriteStartObject("properties");
		writer.WriteString("date", date.ToString("yyyy-MM-dd"));
		writer.WriteEndObject(); // End properties

		writer.WriteEndObject(); // End feature
	}

	#endregion
}
