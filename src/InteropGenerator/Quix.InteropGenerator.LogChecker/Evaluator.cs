using System.Text;

namespace Quix.InteropGenerator;

public class Evaluator
{
    private int allocationCount;
    private int releaseCount;
    private Dictionary<string, LogParser.PtrAllocationLineParser> allocations = new(); //points to last allocation for the ptr
    private Dictionary<string, LogParser.PtrAllocationLineParser> remainingAllocations = new();
    
    private List<(LogParser.PtrFreedLineParser, LogParser.PtrAllocationLineParser)> unnecessaryPtrFree = new();
    private List<(LogParser.ILineParser, string)> oddStuff = new();

    public void Add(LogParser.ILineParser parser)
    {
        try
        {
            switch (parser)
            {
                case LogParser.PtrAllocationLineParser ptrAllocation:
                    allocationCount++;
                    try
                    {
                        remainingAllocations.Add(ptrAllocation.Ptr, ptrAllocation);
                    }
                    catch
                    {
                        oddStuff.Add((parser, $"Allocated pointer {ptrAllocation.Ptr} multiple times without being deallocated first"));
                    }

                    allocations[ptrAllocation.Ptr] = ptrAllocation;
                    break;
                case LogParser.PtrFreedLineParser ptrFreed:
                {
                    releaseCount++;
                    if (!remainingAllocations.Remove(ptrFreed.Ptr))
                    {
                        if (allocations.TryGetValue(ptrFreed.Ptr, out var lastAllocation))
                        {
                            unnecessaryPtrFree.Add((ptrFreed, lastAllocation));
                        }
                        else
                        {
                            oddStuff.Add((ptrFreed, $"Freed pointer {ptrFreed.Ptr} without it being allocated"));
                        }
                    }

                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Line {parser.LineNumber} has exception");
            throw;
        }
    }

    public void Evaluate()
    {
        var summary = new StringBuilder();
        
        EvaluateOddStuff(summary);
        EvaluateRemainingAllocations(summary);
        EvaluateUnnecessaryPtrFreeing(summary);
        
        Console.WriteLine();
        Console.WriteLine("Summary");
        Console.WriteLine($"Total allocation: {allocationCount}");
        Console.WriteLine($"Total allocation release: {releaseCount}");
        Console.WriteLine(summary.ToString());
    }

    private void EvaluateRemainingAllocations(StringBuilder summaryWriter)
    {
        if (!remainingAllocations.Any())
        {
            summaryWriter.AppendLine("No allocation remained. GOOD JOB!");
            return;
        }

        Console.WriteLine("");
        Console.WriteLine("Remaining allocation:");
        if (remainingAllocations.Count > 20)
        {
            Console.WriteLine("(showing only first 20 unique)");
            foreach (var group in remainingAllocations.GroupBy(y => y.Value.Type)
                         .Select(y => new { item = y.First(), count = y.Count() }).OrderByDescending(y=> y.count).Take(20))
            {
                Console.WriteLine($"{group.count} count: {group.item.Value.Ptr} for type {group.item.Value.Type}, example line: {group.item.Value.LineNumber}");
            }
        }
        else
        {
            foreach (var remainingAllocation in remainingAllocations)
            {
                Console.WriteLine($"#{remainingAllocation.Value.LineNumber} for type {remainingAllocation.Value.Type}");
            }
        }

        summaryWriter.AppendLine($"There are {remainingAllocations.Count} remaining allocations");
    }

    private void EvaluateUnnecessaryPtrFreeing(StringBuilder summaryWriter)
    {
        if (!unnecessaryPtrFree.Any())
        {
            summaryWriter.AppendLine("No unnecessary ptr freed. GOOD JOB!");
            return;
        }
        Console.WriteLine("");
        Console.WriteLine("Ptrs freed multiple times:");

        if (unnecessaryPtrFree.Count > 20)
        {
            Console.WriteLine("(showing only first 20 unique)");
            foreach (var group in unnecessaryPtrFree.GroupBy(y => y.Item2.Type)
                         .Select(y => new { item = y.First(), count = y.Count() }).OrderByDescending(y=> y.count).Take(20))
            {
                Console.WriteLine($"{group.count} count for type {group.item.Item2.Type}, example line: {group.item.Item1.LineNumber}");
            }
        }
        else
        {

            foreach (var (unnecessaryPtrFree, lastPtrAllocation) in unnecessaryPtrFree)
            {
                Console.WriteLine(
                    $"#{unnecessaryPtrFree.LineNumber}: {unnecessaryPtrFree.Ptr} for type {lastPtrAllocation.Type}");
            }
        }

        summaryWriter.AppendLine($"There are {unnecessaryPtrFree.Count} unnecessary ptr releases");
    }


    private void EvaluateOddStuff(StringBuilder summaryWriter)
    {
        if (!oddStuff.Any())
        {
            summaryWriter.AppendLine("No odd thing found. GOOD JOB!");
            return;
        }
        Console.WriteLine("Odd things found");
        var printing = oddStuff;
        if (oddStuff.Count > 20)
        {
            printing = oddStuff.Take(20).ToList();
            Console.WriteLine("(showing only first 20)");
        }
        foreach (var oddStuff in printing)
        {
            Console.WriteLine($"#{oddStuff.Item1.LineNumber}: {oddStuff.Item2}");
        }
        summaryWriter.AppendLine($"There are {oddStuff.Count} odd stuffs");
    }
}