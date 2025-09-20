<?php

namespace MHz\MysqlVector\Tests;

use MHz\MysqlVector\VectorTable;

/**
 * SearchEffectivenessTest - Semantic search quality validation
 *
 * This test class validates the search effectiveness and accuracy of the VectorTable::search() method.
 *
 * ## Features
 * - **Search Quality Validation**: Verifies that semantically similar content ranks higher than unrelated content
 * - **Semantic Category Testing**: Tests across all available categories dynamically loaded from test data
 * - **Ranking Quality Assertions**: Ensures more similar content consistently ranks higher
 * - **Ground Truth Verification**: Compares search results against exact cosine similarity search results computed purely in PHP
 * - **Edge Case Testing**: Tests exact matches, partial matches, and unrelated queries
 * - **Pre-generated Vectors**: Uses high-quality 3072-dimension vectors for consistent testing
 * - **Zero External Dependencies**: All test data self-contained with pre-computed vectors
 *
 * ## JSON Data Structure
 * The test uses a test data JSON file (`vectors.json`) with the following format:
 * ```json
 * [
 *   {
 *     "text": "Machine learning algorithms optimize neural network performance",
 *     "vector": [0.1, -0.2, 0.3, ...],
 *     "semantic_category": "technology",
 *     "usage": ["semantic_similarity", "edge_cases", "performance_testing"]
 *   }
 * ]
 * ```
 *
 * **Important**: Every entry must have a non-null vector. The test automatically
 * selects diverse texts from different categories to use as test queries.
 *
 * ## Customizing Test Data
 * To modify test scenarios, edit the `vectors.json` file:
 *
 * ### Adding New Test Texts:
 * ```json
 * {
 *   "text": "Your new test text here",
 *   "vector": [your_vector_array],
 *   "semantic_category": "your_category",
 *   "usage": ["semantic_similarity"]
 * }
 * ```
 * 
 */

class SearchEffectivenessTest extends BaseVectorTest
{
    // Configuration
    private int $vectorDimension = 3072;

    // Test data storage
    private array $testData = [];
    private array $categorizedVectors = [];

    protected function setUp(): void
    {
        parent::setUp();

        echo "\n=== MySQL Vector Library - Search Effectiveness Test ===\n";
        echo "Vector dimensions: {$this->vectorDimension}\n";
        echo "Pre-computed vectors: " . count($this->getTestData()) . " total\n\n";
    }

    /**
     * Get test data with lazy loading
     * Loads and parses vectors.json file only when first accessed
     */
    private function getTestData(): array
    {
        if (empty($this->testData)) {
            $vectorsFile = __DIR__ . '/vectors.json';
            if (!file_exists($vectorsFile)) {
                throw new \Exception('Pre-computed vectors file not found. Ensure vectors.json exists in tests/ directory.');
            }

            $jsonContent = file_get_contents($vectorsFile);
            $this->testData = json_decode($jsonContent, true);

            if ($this->testData === null) {
                throw new \Exception('Failed to decode vectors.json. Invalid JSON format.');
            }

            // Validate JSON structure and organize data
            $this->categorizedVectors = [];
            foreach ($this->testData as $index => $entry) {
                // Validate required fields
                if (!isset($entry['text']) || !isset($entry['semantic_category']) || !isset($entry['usage'])) {
                    throw new \Exception("Invalid JSON structure for entry at index '$index'. Missing required fields: text, semantic_category, usage");
                }

                if (!isset($entry['vector']) || (!is_array($entry['vector']) && $entry['vector'] !== null)) {
                    throw new \Exception("Invalid JSON structure for entry at index '$index'. Missing or invalid 'vector' field");
                }

                if (!is_array($entry['usage'])) {
                    throw new \Exception("Invalid JSON structure for entry at index '$index'. usage must be an array");
                }

                // Validate vector dimensions
                if (is_array($entry['vector']) && count($entry['vector']) !== $this->vectorDimension) {
                    throw new \Exception("Invalid vector dimensions for entry at index '$index'. Expected {$this->vectorDimension}, got " . count($entry['vector']));
                }

                // Organize by category
                $category = $entry['semantic_category'];
                if (!isset($this->categorizedVectors[$category])) {
                    $this->categorizedVectors[$category] = [];
                }
                $this->categorizedVectors[$category][$index] = $entry;
            }

            // Validate that we have sufficient category diversity
            $categories = array_keys($this->categorizedVectors);
            if (count($categories) < 3) {
                throw new \Exception("Insufficient category diversity. Expected at least 3 categories, found " . count($categories) . ": " . implode(', ', $categories));
            }

            echo "Using pre-generated vectors (no external dependencies)\n";
            echo "Loaded " . count($this->testData) . " structured entries\n";
        }

        return $this->testData;
    }

    /**
     * Get vector entries by their usage criteria
     * Returns complete entry objects, not just text strings
     */
    private function getVectorsByUsage(string $usageType): array
    {
        $testData = $this->getTestData();
        return array_filter($testData, function($entry) use ($usageType) {
            return isset($entry['usage']) && in_array($usageType, $entry['usage']);
        }, ARRAY_FILTER_USE_BOTH);
    }

    /**
     * Get vector entries by semantic category
     * Returns complete entry objects, not just text strings
     */
    private function getVectorsByCategory(string $category, ?int $limit = null): array
    {
        if (!isset($this->categorizedVectors[$category])) {
            return [];
        }

        $categoryEntries = $this->categorizedVectors[$category];
        return $limit ? array_slice($categoryEntries, 0, $limit, true) : $categoryEntries;
    }

    /**
     * Get all available semantic categories from the loaded data
     * Returns array of category names sorted alphabetically
     */
    private function getAvailableCategories(): array
    {
        return array_keys($this->categorizedVectors);
    }

    /**
     * Load test vectors into the database for search effectiveness testing
     *
     * @param VectorTable $vectorTable The VectorTable instance to use
     * @param string $usage The test usage type to filter vectors by (e.g., 'semantic_similarity', 'edge_cases')
     */
    private function setupVectorData(VectorTable $vectorTable, string $usage): void
    {
        // Get test entries based on usage type
        $testEntries = $this->getVectorsByUsage($usage);
        if (empty($testEntries)) {
            throw new \Exception("No test texts found for usage type: '$usage'");
        }

        echo "Loading " . count($testEntries) . " vectors for '$usage' testing...\n";
        echo "Storing vectors in database...\n";

        $vectorTable->getConnection()->begin_transaction();

        try {
            $items = [];
            foreach ($testEntries as $index => $entry) {
                $items[] = [
                    'vector' => $entry['vector'],
                    'metadata' => [
                        'test_index' => $index,
                        'semantic_category' => $entry['semantic_category'],
                        'text' => $entry['text'],
                        'usage' => $entry['usage'] ?? []
                    ],
                ];
            }

            // Store vectors with metadata in a single batch
            $vectorTable->batchInsert($items);

            $vectorTable->getConnection()->commit();
            echo "Successfully stored " . count($testEntries) . " vectors.\n";
            echo "✓ Test data loaded: " . count($testEntries) . " vectors stored\n\n";

        } catch (\Exception $e) {
            $vectorTable->getConnection()->rollback();
            throw new \Exception("Failed to store vectors: " . $e->getMessage());
        }
    }

    /**
     * Clean up vectors from database after testing
     *
     * @param VectorTable $vectorTable The VectorTable instance to clean up
     */
    private function cleanupVectorData(VectorTable $vectorTable): void
    {
        // Clear all vectors from the test table
        $tableName = $vectorTable->getVectorTableName();
        $result = $vectorTable->getConnection()->query("DELETE FROM `$tableName`");

        if (!$result) {
            echo "! Warning: Failed to clean up test data\n\n";
            return;
        }

        echo "✓ Test data cleaned up\n\n";
    }

    /**
     * Test: Semantic similarity search effectiveness
     * Tests the library's ability to find semantically similar content
     */
    public function testSemanticSimilaritySearch(): void
    {
        echo "=== Semantic Similarity Search Test ===\n";

        // Create VectorTable for this test
        $vectorTable = $this->makeTable('semantic_similarity_test', $this->vectorDimension);

        // Setup test data for semantic similarity testing
        $this->setupVectorData($vectorTable, 'semantic_similarity');

        // Get semantic test queries from structured data
        $semanticTestCases = [];

        // Select representative texts from all available categories for semantic testing
        $targetCategories = $this->getAvailableCategories();

        foreach ($targetCategories as $category) {
            // Get entries from this category that are marked for semantic similarity testing
            $categoryEntries = $this->getVectorsByCategory($category);

            foreach ($categoryEntries as $index => $entry) {
                // Only include entries that are marked for semantic similarity testing
                if (isset($entry['usage']) && in_array('semantic_similarity', $entry['usage'])) {
                    $semanticTestCases[] = [
                        'query' => $entry['text'],
                        'expected_categories' => [$category],
                        'semantic_category' => $entry['semantic_category'],
                        'entry' => $entry  // Include full entry for access to other properties
                    ];
                    // Only take one entry per category to keep test execution reasonable
                    break;
                }
            }
        }

        $totalTests = 0;
        $passedTests = 0;

        foreach ($semanticTestCases as $testCase) {
            echo "Testing query: '{$testCase['query']}'\n";

            // Get vector for query
            $queryVector = $testCase['entry']['vector'];

            // Search for similar vectors
            $startTime = microtime(true);
            $results = $vectorTable->search($queryVector, null, 10);
            $searchTime = microtime(true) - $startTime;

            echo "  Search completed in " . number_format($searchTime * 1000, 2) . "ms\n";
            echo "  Top 5 results:\n";

            // Assert that search returns results
            $this->assertNotEmpty($results, "Search should return results for query: '{$testCase['query']}'");

            $foundMatches = 0;
            foreach (array_slice($results, 0, 5) as $index => $result) {
                $this->assertIsArray($result, "Search result item must be array");
                $this->assertArrayHasKey('similarity', $result, "Search result missing 'similarity'");
                $meta = $result['metadata'] ?? null;
                $this->assertIsArray($meta, "Search result missing metadata");
                $this->assertArrayHasKey('semantic_category', $meta, "Search result metadata missing 'semantic_category'");
                $this->assertArrayHasKey('text', $meta, "Search result metadata missing 'text'");
                $category = $meta['semantic_category'];
                $text = (string)$meta['text'];

                $similarity = $result['similarity'];
                echo "    " . ($index + 1) . ". [" . number_format($similarity, 4) . "] " . substr($text, 0, 60) . "... category=" . (string)$category . "\n";

                // Check if this result matches expected categories
                if ($category && in_array($category, $testCase['expected_categories'])) {
                    $foundMatches++;
                }
            }

            // Assert search quality - should find relevant matches and have good top similarity
            $topSimilarity = $results[0]['similarity'];
            $this->assertGreaterThan(0.5, $topSimilarity, "Top result should have similarity > 0.5 for query: '{$testCase['query']}'");
            $this->assertGreaterThan(0, $foundMatches, "Should find at least one relevant match for query: '{$testCase['query']}'");

            if ($foundMatches > 0 && $topSimilarity > 0.5) {
                echo "  ✓ Test passed (found $foundMatches relevant matches)\n";
                $passedTests++;
            } else {
                echo "  ✗ Test failed (found $foundMatches relevant matches, top similarity: " .
                     number_format($topSimilarity, 4) . ")\n";
            }

            $totalTests++;
            echo "\n";
        }

        $successRate = ($passedTests / $totalTests) * 100;
        echo "Semantic similarity test results: $passedTests/$totalTests passed (" . number_format($successRate, 1) . "%)\n";

        // Assert minimum success rate
        $this->assertGreaterThanOrEqual(60, $successRate, "Semantic similarity success rate should be at least 60%");

        echo "✓ Semantic similarity success rate meets expectations\n";
        echo "✓ Semantic similarity search test completed\n\n";

        // Clean up test data
        $this->cleanupVectorData($vectorTable);
    }

    /**
     * Test: VectorTable::search correctness vs ground truth
     * Compares search() results against exact computed cosine similarities using VectorTable::cosim() and PHP's usort().
     * Uses the same semantic test cases as testSemanticSimilaritySearch.
     */
    public function testSearchMatchesGroundTruth(): void
    {
        echo "=== Search Correctness vs Ground Truth ===\n";

        // Create VectorTable and load semantic_similarity data
        $vectorTable = $this->makeTable('correctness_validation_test', $this->vectorDimension);
        $this->setupVectorData($vectorTable, 'semantic_similarity');

        // Build test cases
        $semanticTestCases = [];
        $targetCategories = $this->getAvailableCategories();
        foreach ($targetCategories as $category) {
            $categoryEntries = $this->getVectorsByCategory($category);
            foreach ($categoryEntries as $entry) {
                if (isset($entry['usage']) && in_array('semantic_similarity', $entry['usage'])) {
                    $semanticTestCases[] = [
                        'query' => $entry['text'],
                        'semantic_category' => $entry['semantic_category'],
                        'entry' => $entry,
                    ];
                    break; // one per category keeps runtime reasonable
                }
            }
        }

        $allStored = $this->getVectorsByUsage('semantic_similarity');
        $this->assertNotEmpty($allStored, 'Expected non-empty stored vector set for semantic_similarity');

        $topN = 10;
        $epsilon = 1e-6;

        foreach ($semanticTestCases as $case) {
            echo "Query: '" . $case['query'] . "'\n";
            $queryVector = $case['entry']['vector'];

            // Database search results
            $dbResults = $vectorTable->search($queryVector, null, $topN);
            $this->assertNotEmpty($dbResults, 'search() should return results');

            // Ground truth: cosine similarity against all stored vectors (exact)
            $gt = [];
            foreach ($allStored as $idx => $entry) {
                $gt[] = [
                    'test_index' => $idx,
                    'similarity' => $vectorTable->cosim($queryVector, $entry['vector']),
                    'text' => $entry['text'],
                ];
            }
            usort($gt, static function($a, $b) {
                if ($a['similarity'] === $b['similarity']) return 0;
                return ($a['similarity'] < $b['similarity']) ? 1 : -1;
            });
            $gtTop = array_slice($gt, 0, min($topN, count($gt)));

            // Build comparable sequences
            $dbTopCount = min($topN, count($dbResults));
            $this->assertSame($dbTopCount, count($gtTop), 'Top-N counts should match');

            $dbIndices = [];
            $dbSims = [];
            for ($i = 0; $i < $dbTopCount; $i++) {
                $this->assertArrayHasKey('metadata', $dbResults[$i], 'Result missing metadata');
                $this->assertArrayHasKey('similarity', $dbResults[$i], 'Result missing similarity');
                $this->assertArrayHasKey('test_index', $dbResults[$i]['metadata'], 'Metadata missing test_index');
                $dbIndices[] = $dbResults[$i]['metadata']['test_index'];
                $dbSims[] = $dbResults[$i]['similarity'];
            }

            $gtIndices = array_column($gtTop, 'test_index');
            $gtSims = array_column($gtTop, 'similarity');

            // Validate order: non-increasing similarity sequence from search()
            for ($i = 0; $i < $dbTopCount - 1; $i++) {
                $this->assertGreaterThanOrEqual(
                    $dbSims[$i+1] - $epsilon,
                    $dbSims[$i],
                    "Results must be sorted by similarity (desc) at positions $i and " . ($i+1)
                );
            }

            // Validate that the top-N sets match (ignoring tie ordering)
            $sortedDbIdx = $dbIndices; sort($sortedDbIdx);
            $sortedGtIdx = $gtIndices; sort($sortedGtIdx);
            $this->assertSame(
                $sortedGtIdx,
                $sortedDbIdx,
                'Top-N result identity must match ground truth (set equality)'
            );

            // Validate similarity values within tolerance (as multisets)
            $sortedDbSims = $dbSims; rsort($sortedDbSims);
            $sortedGtSims = $gtSims; rsort($sortedGtSims);
            $this->assertSame(count($sortedGtSims), count($sortedDbSims), 'Similarity list lengths must match');
            for ($i = 0; $i < count($sortedDbSims); $i++) {
                $this->assertLessThanOrEqual(
                    $epsilon,
                    abs($sortedDbSims[$i] - $sortedGtSims[$i]),
                    "Similarity mismatch at rank $i: DB={$sortedDbSims[$i]} vs GT={$sortedGtSims[$i]}"
                );
            }

            echo "  ✓ Ground truth match for top-{$dbTopCount}\n\n";
        }

        // Clean up test data
        $this->cleanupVectorData($vectorTable);
    }

    /**
     * Test: Edge cases and boundary conditions
     * Tests exact matches, partial matches, and completely unrelated queries
     */
    public function testEdgeCases(): void
    {
        echo "=== Edge Case Testing ===\n";

        // Create VectorTable for this test
        $vectorTable = $this->makeTable('edge_cases_test', $this->vectorDimension);

        // Setup test data for edge case testing (stores entries with 'edge_cases' usage)
        $this->setupVectorData($vectorTable, 'edge_cases');

        // Build edge case test queries using entries with 'edge_cases_queries' usage
        // These entries are NOT stored in the database, avoiding exact matches
        $edgeCaseQueries = [];
        $queryEntries = $this->getVectorsByUsage('edge_cases_queries');

        // Get available categories from stored entries to determine expected matches
        $storedEntries = $this->getVectorsByUsage('edge_cases');
        $availableCategories = array_unique(array_column($storedEntries, 'semantic_category'));

        // High similarity test - use first available query entry
        if (!empty($queryEntries)) {
            $highSimEntry = reset($queryEntries);
            $expectedCategories = in_array($highSimEntry['semantic_category'], $availableCategories)
                ? [$highSimEntry['semantic_category']]
                : $availableCategories; // Fallback to all categories

            $edgeCaseQueries[] = [
                'query' => $highSimEntry['text'],
                'test_type' => 'high_similarity',
                'expected_categories' => $expectedCategories,
                'semantic_category' => $highSimEntry['semantic_category'],
                'entry' => $highSimEntry
            ];
        }

        // Partial match test - use second available query entry if different category
        if (count($queryEntries) > 1) {
            $partialMatchEntry = null;
            $firstCategory = reset($queryEntries)['semantic_category'];

            foreach ($queryEntries as $entry) {
                if ($entry['semantic_category'] !== $firstCategory) {
                    $partialMatchEntry = $entry;
                    break;
                }
            }

            // If no different category found, use second entry anyway
            if (!$partialMatchEntry && count($queryEntries) > 1) {
                $partialMatchEntry = $queryEntries[array_keys($queryEntries)[1]];
            }

            if ($partialMatchEntry) {
                // For partial match, focus on similarity scores rather than category predictions
                // Allow matches from any available category since we can't predict semantic relationships
                $edgeCaseQueries[] = [
                    'query' => $partialMatchEntry['text'],
                    'test_type' => 'partial_match',
                    'expected_categories' => $availableCategories, // Allow matches from any category
                    'semantic_category' => $partialMatchEntry['semantic_category'],
                    'entry' => $partialMatchEntry
                ];
            }
        }

        // Unrelated query test - use entry from category with fewest stored entries or different from used categories
        $unrelatedQueryEntry = null;
        $usedCategories = array_column($edgeCaseQueries, 'semantic_category');

        foreach ($queryEntries as $entry) {
            $category = $entry['semantic_category'];
            // Use entry from category not already used in other tests
            if (!in_array($category, $usedCategories)) {
                $unrelatedQueryEntry = $entry;
                break;
            }
        }

        // Fallback: use any remaining entry
        if (!$unrelatedQueryEntry && count($queryEntries) > count($edgeCaseQueries)) {
            foreach ($queryEntries as $entry) {
                $alreadyUsed = false;
                foreach ($edgeCaseQueries as $existingQuery) {
                    if ($existingQuery['entry']['text'] === $entry['text']) {
                        $alreadyUsed = true;
                        break;
                    }
                }
                if (!$alreadyUsed) {
                    $unrelatedQueryEntry = $entry;
                    break;
                }
            }
        }

        if ($unrelatedQueryEntry) {
            $edgeCaseQueries[] = [
                'query' => $unrelatedQueryEntry['text'],
                'test_type' => 'unrelated_query',
                'expected_categories' => [], // No specific categories expected for unrelated queries
                'semantic_category' => $unrelatedQueryEntry['semantic_category'],
                'entry' => $unrelatedQueryEntry
            ];
        }

        foreach ($edgeCaseQueries as $testCase) {
            echo "Test: {$testCase['test_type']} - '{$testCase['query']}'\n";

            $queryVector = $testCase['entry']['vector'];
            $results = $vectorTable->search($queryVector, null, 5);

            // Assert that search returns results
            $this->assertNotEmpty($results, "Search should return results for edge case: '{$testCase['query']}'");

            $topSimilarity = $results[0]['similarity'];
            echo "  Top similarity: " . number_format($topSimilarity, 4) . "\n";

            // Apply test-specific validation based on test type
            switch ($testCase['test_type']) {
                case 'high_similarity':
                    $this->assertGreaterThan(0.3, $topSimilarity, "High similarity test should have similarity > 0.3 for query: '{$testCase['query']}'");
                    echo "  ✓ High similarity test passed (similarity: " . number_format($topSimilarity, 6) . ")\n";
                    break;

                case 'unrelated_query':
                    $maxSimilarity = max(array_column($results, 'similarity'));
                    $this->assertLessThan(0.6, $maxSimilarity, "Unrelated query should have max similarity < 0.6 for query: '{$testCase['query']}'");
                    echo "  ✓ Unrelated query test passed (max similarity: " . number_format($maxSimilarity, 4) . ")\n";
                    break;

                case 'partial_match':
                    $this->assertGreaterThan(0.2, $topSimilarity, "Partial match should have similarity > 0.2 for query: '{$testCase['query']}'");
                    $this->assertLessThan(0.8, $topSimilarity, "Partial match should have similarity < 0.8 for query: '{$testCase['query']}'");
                    echo "  ✓ Partial match test passed (similarity: " . number_format($topSimilarity, 4) . ")\n";
                    break;

                default:
                    echo "  ✓ Edge case test completed (similarity: " . number_format($topSimilarity, 4) . ")\n";
            }
            echo "\n";
        }

        echo "✓ Edge case tests completed\n\n";

        // Clean up test data
        $this->cleanupVectorData($vectorTable);
    }
}
