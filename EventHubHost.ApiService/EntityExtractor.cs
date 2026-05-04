using System.Text.RegularExpressions;

namespace EventHubHost.ApiService;

/// <summary>
/// Lightweight regex-based entity extractor that pulls source-system mentions and event-type numbers
/// out of a free-text question. Used to build a Qdrant payload filter for hybrid retrieval — the
/// vector search is restricted to the systems/types the user actually mentioned, dramatically
/// improving precision over pure semantic similarity.
/// </summary>
public static partial class EntityExtractor
{
    [GeneratedRegex(@"\bsystem\s*([ab])\b", RegexOptions.IgnoreCase)]
    private static partial Regex SystemRegex();

    [GeneratedRegex(@"\b(?:type|event\s*type|event)\s*(\d{1,5})\b", RegexOptions.IgnoreCase)]
    private static partial Regex TypeRegex();

    [GeneratedRegex(@"\b(\d{4,5})\b")]
    private static partial Regex BareTypeRegex();

    public static QuestionEntities Extract(string question)
    {
        if (string.IsNullOrWhiteSpace(question))
        {
            return new QuestionEntities([], []);
        }

        var systems = SystemRegex()
            .Matches(question)
            .Select(match => $"System {match.Groups[1].Value.ToUpperInvariant()}")
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var types = TypeRegex()
            .Matches(question)
            .Select(match => int.TryParse(match.Groups[1].ValueSpan, out var type) ? type : -1)
            .Where(type => type >= 0)
            .Concat(BareTypeRegex()
                .Matches(question)
                .Select(match => int.TryParse(match.Groups[1].ValueSpan, out var type) ? type : -1)
                .Where(type => type >= 1000))
            .Distinct()
            .ToArray();

        return new QuestionEntities(systems, types);
    }
}

public sealed record QuestionEntities(IReadOnlyList<string> SourceSystems, IReadOnlyList<int> EventTypes)
{
    public bool HasFilters => SourceSystems.Count > 0 || EventTypes.Count > 0;
}
