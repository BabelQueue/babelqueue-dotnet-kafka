namespace BabelQueue.Kafka;

/// <summary>
/// The SDK-owned retry/delay topology for one work topic (Contract §6.4–§6.5). Kafka has no
/// native delay, DLQ, or per-message retry, so BabelQueue layers them on tiered delay topics
/// <c>&lt;topic&gt;.retry.&lt;n&gt;</c> (each mapped to a delay tier, ascending) plus an opt-in
/// <c>&lt;topic&gt;.dlq</c>. A delay or a release with no tiers configured raises rather than
/// silently dropping.
/// </summary>
public sealed class RetryTopics
{
    /// <summary>A single delay tier: the <c>&lt;topic&gt;.retry.&lt;n&gt;</c> topic and the delay it holds for.</summary>
    public sealed record Tier(string Topic, TimeSpan Delay);

    /// <summary>
    /// The RECOMMENDED tier delays in milliseconds — 5s, 30s, 5m, 30m (ADR-0034 §2). Opt-in: nothing uses it unless
    /// the caller picks it, e.g. <see cref="Builder.RecommendedTiers"/>; the default topology is unchanged.
    /// </summary>
    public static IReadOnlyList<int> RecommendedTierMillis { get; } = new[] { 5_000, 30_000, 300_000, 1_800_000 };

    private readonly List<Tier> _tiers;

    private RetryTopics(string workTopic, List<Tier> tiers, string? dlqTopic)
    {
        WorkTopic = workTopic;
        _tiers = tiers;
        DlqTopic = dlqTopic;
    }

    public string WorkTopic { get; }

    /// <summary>The DLQ topic, or null if dead-lettering is disabled.</summary>
    public string? DlqTopic { get; }

    public IReadOnlyList<Tier> Tiers => _tiers;

    public bool HasTiers => _tiers.Count > 0;

    /// <summary>Start a topology for <paramref name="workTopic"/> (DLQ defaults to <c>&lt;workTopic&gt;.dlq</c>).</summary>
    public static Builder ForTopic(string workTopic) => new(workTopic);

    /// <summary>The smallest tier whose delay ≥ <paramref name="delay"/>; raises if none or too large (§6.4).</summary>
    public Tier TierForDelay(TimeSpan delay)
    {
        RequireTiers();
        foreach (var tier in _tiers)
        {
            if (tier.Delay >= delay)
            {
                return tier;
            }
        }
        throw new BabelQueueException(
            $"Requested Kafka delay {delay.TotalMilliseconds}ms exceeds the largest retry tier ({_tiers[^1].Delay.TotalMilliseconds}ms).");
    }

    /// <summary>
    /// The tier for a retry whose <c>attempts_before</c> is <paramref name="attempt"/> (0-based): tier number
    /// <c>min(attempts_before, N-1) + 1</c>, so an exhausted list stays on the last tier (ADR-0034 §3, retry rule).
    /// Deliberately separate from <see cref="TierForDelay"/>: a delay request never clamps, a retry always does.
    /// </summary>
    public Tier TierForAttempt(int attempt)
    {
        RequireTiers();
        return _tiers[Math.Min(Math.Max(attempt, 0), _tiers.Count - 1)];
    }

    private void RequireTiers()
    {
        if (_tiers.Count == 0)
        {
            throw new BabelQueueException($"Kafka retry/delay requires retry topics; none are configured for '{WorkTopic}'.");
        }
    }

    /// <summary>Builder for <see cref="RetryTopics"/>; tiers may be added in any order (sorted by delay on build).</summary>
    public sealed class Builder
    {
        private readonly string _workTopic;
        private readonly List<TimeSpan> _delays = new();
        private string? _dlqTopic;

        internal Builder(string workTopic)
        {
            _workTopic = workTopic;
            _dlqTopic = workTopic + ".dlq";
        }

        /// <summary>Add a delay tier; tiers are numbered <c>.retry.1</c>, <c>.retry.2</c>, … by ascending delay.</summary>
        public Builder Tier(TimeSpan delay)
        {
            _delays.Add(delay);
            return this;
        }

        /// <summary>Add the RECOMMENDED tiers (<see cref="RecommendedTierMillis"/>) — an explicit opt-in.</summary>
        public Builder RecommendedTiers()
        {
            foreach (var millis in RecommendedTierMillis)
            {
                _delays.Add(TimeSpan.FromMilliseconds(millis));
            }
            return this;
        }

        public Builder DlqTopic(string dlqTopic)
        {
            _dlqTopic = dlqTopic;
            return this;
        }

        /// <summary>Disable dead-lettering — terminal failures degrade to commit-and-drop (§6.5).</summary>
        public Builder WithoutDlq()
        {
            _dlqTopic = null;
            return this;
        }

        public RetryTopics Build()
        {
            var sorted = _delays.OrderBy(d => d).ToList();
            var tiers = new List<Tier>(sorted.Count);
            for (var i = 0; i < sorted.Count; i++)
            {
                tiers.Add(new Tier($"{_workTopic}.retry.{i + 1}", sorted[i]));
            }
            return new RetryTopics(_workTopic, tiers, _dlqTopic);
        }
    }
}
