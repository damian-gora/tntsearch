<?php

namespace TeamTNT\TNTSearchASFW;

use PDO;
use TeamTNT\TNTSearchASFW\Exceptions\IndexNotFoundException;
use TeamTNT\TNTSearchASFW\Indexer\TNTIndexer;
use TeamTNT\TNTSearchASFW\Stemmer\PorterStemmer;
use TeamTNT\TNTSearchASFW\Support\Collection;
use TeamTNT\TNTSearchASFW\Support\Expression;
use TeamTNT\TNTSearchASFW\Support\Highlighter;
use TeamTNT\TNTSearchASFW\Support\Tokenizer;
use TeamTNT\TNTSearchASFW\Support\TokenizerInterface;
use TeamTNT\TNTSearchASFW\Connectors\MySqlConnector;
use DgoraWcas\Engines\TNTSearchMySQL\Indexer\Searchable\Database;

class TNTSearch
{
    public $config;
    public $asYouType = false;
    public $maxDocs = 500;
    public $tokenizer = null;
    public $index = null;
    public $stemmer = null;
    public $fuzziness = false;
    public $fuzzy_prefix_length = 2;
    public $fuzzy_max_expansions = 50;
    public $fuzzy_distance = 2;
    protected $lang = '';
    protected $postType = '';
    protected $dbh = null;

    /**
     * @param array $config
     *
     * @see https://github.com/teamtnt/tntsearch#examples
     */
    public function loadConfig(array $config)
    {
        $this->config            = $config;
        $this->config['storage'] = rtrim($this->config['storage'], '/') . '/';
    }

    public function __construct()
    {
        $this->tokenizer = new Tokenizer;
    }

    /**
     * @param PDO $dbh
     */
    public function setDatabaseHandle(PDO $dbh)
    {
        $this->dbh = $dbh;
    }

    /**
     * Set lang
     *
     * @param string $lang
     */
    public function setLang($lang)
    {
        if (
            ! empty($lang)
            && is_string($lang)
            && preg_match('/^[a-z]{2}$/', $lang)
        ) {
            $this->lang = $lang;
        }
    }

    /**
     * Set post type
     *
     * @param string $postType
     */
    public function setPostType($postType)
    {
        if (
            ! empty($postType)
            && is_string($postType)
            && preg_match('/^[a-z_\-]{1,50}$/', $postType)
        ) {
            $this->postType = $postType;
        }
    }

    /**
     * @param TokenizerInterface $tokenizer
     */
    public function setTokenizer(TokenizerInterface $tokenizer)
    {
        $this->tokenizer = $tokenizer;
    }

    /**
     * @param string $indexName
     * @param boolean $disableOutput
     *
     * @return TNTIndexer
     */
    public function createIndex($deprecated = '', $disableOutput = false)
    {

        $indexer = new TNTIndexer;
        $indexer->loadConfig($this->config);
        $indexer->disableOutput = $disableOutput;

        if ($this->dbh) {
            $indexer->setDatabaseHandle($this->dbh);
        }

        return $indexer->createIndex();
    }

    /**
     * @throws IndexNotFoundException
     */
    public function selectIndex()
    {
        $pdo         = new MySqlConnector();
        $this->index = $pdo->connect(Database::getConfig());

        $this->setStemmer();
    }

    /**
     * @param string $phrase
     * @param int $numOfResults
     *
     * @return array
     */
    public function search($phrase, $numOfResults = 100)
    {
        $startTimer = microtime(true);
        $keywords   = $this->breakIntoTokens($phrase);
        $keywords   = new Collection($keywords);

        $keywords = $keywords->map(function ($keyword) {
            return $this->stemmer->stem($keyword);
        });

        $tfWeight  = 1;
        $dlWeight  = 0.5;
        $docScores = [];
        $count     = $this->totalDocumentsInCollection();

        foreach ($keywords as $index => $term) {
            $isLastKeyword = ($keywords->count() - 1) == $index;
            $df            = $this->totalMatchingDocuments($term, $isLastKeyword);
            $idf           = log($count / max(1, $df));
            foreach ($this->getAllDocumentsForKeyword($term, false, $isLastKeyword) as $document) {
                $docID             = $document['doc_id'];
                $tf                = $document['hit_count'];
                $num               = ($tfWeight + 1) * $tf;
                $denom             = $tfWeight
                                     * ((1 - $dlWeight) + $dlWeight)
                                     + $tf;
                $score             = $idf * ($num / $denom);
                $docScores[$docID] = isset($docScores[$docID]) ?
                    $docScores[$docID] + $score : $score;
            }
        }

        arsort($docScores);

        $docs = new Collection($docScores);

        $totalHits = $docs->count();
        $docs      = $docs->map(function ($doc, $key) {
            return $key;
        })->take($numOfResults);
        $stopTimer = microtime(true);

        if ($this->isFileSystemIndex()) {
            return $this->filesystemMapIdsToPaths($docs)->toArray();
        }

        return [
            'ids'            => array_keys($docs->toArray()),
            'hits'           => $totalHits,
            'execution_time' => round($stopTimer - $startTimer, 7) * 1000 . " ms"
        ];
    }

    /**
     * @param string $phrase
     * @param int $numOfResults
     *
     * @return array
     */
    public function searchBoolean($phrase, $numOfResults = 100)
    {

        $keywords    = $this->breakIntoTokens($phrase);
        $lastKeyword = end($keywords);

        $stack      = [];
        $startTimer = microtime(true);

        $expression = new Expression;

        $postfix = !empty($keywords) ? $expression->toPostfix("|" . implode(' ', $keywords)) : array();
        
        foreach ($postfix as $token) {
            if ($token == '&') {
                $left  = array_pop($stack);
                $right = array_pop($stack);
                if (is_string($left)) {
                    $isLastKeyword = $left == $lastKeyword;
                    $left          = $this->getAllDocumentsForKeyword($this->stemmer->stem($left), true, $isLastKeyword)
                                          ->pluck('doc_id');
                }
                if (is_string($right)) {
                    $isLastKeyword = $right == $lastKeyword;
                    $right         = $this->getAllDocumentsForKeyword($this->stemmer->stem($right), true, $isLastKeyword)
                                          ->pluck('doc_id');
                }
                if (is_null($left)) {
                    $left = [];
                }

                if (is_null($right)) {
                    $right = [];
                }
                $stack[] = array_values(array_intersect($left, $right));
            } elseif ($token == '|') {
                $left  = array_pop($stack);
                $right = array_pop($stack);

                if (is_string($left)) {
                    $isLastKeyword = $left == $lastKeyword;
                    $left          = $this->getAllDocumentsForKeyword($this->stemmer->stem($left), true, $isLastKeyword)
                                          ->pluck('doc_id');
                }

                if (is_string($right)) {
                    $isLastKeyword = $right == $lastKeyword;
                    $right         = $this->getAllDocumentsForKeyword($this->stemmer->stem($right), true, $isLastKeyword)
                                          ->pluck('doc_id');
                }

                if (is_null($left)) {
                    $left = [];
                }

                if (is_null($right)) {
                    $right = [];
                }
                $stack[] = array_unique(array_merge($left, $right));
            } elseif ($token == '~') {
                $left = array_pop($stack);
                if (is_string($left)) {
                    $left = $this->getAllDocumentsForWhereKeywordNot($this->stemmer->stem($left), true)
                                 ->pluck('doc_id');
                }
                if (is_null($left)) {
                    $left = [];
                }
                $stack[] = $left;
            } else {
                $stack[] = $token;
            }
        }
        if (count($stack)) {
            $docs = new Collection($stack[0]);
        } else {
            $docs = new Collection;
        }

        $docs = $docs->take($numOfResults);

        $stopTimer = microtime(true);

        if ($this->isFileSystemIndex()) {
            return $this->filesystemMapIdsToPaths($docs)->toArray();
        }

        return [
            'ids'            => $docs->toArray(),
            'hits'           => $docs->count(),
            'execution_time' => round($stopTimer - $startTimer, 7) * 1000 . " ms"
        ];
    }

    /**
     * @param      $keyword
     * @param bool $noLimit
     * @param bool $isLastKeyword
     *
     * @return Collection
     */
    public function getAllDocumentsForKeyword($keyword, $noLimit = false, $isLastKeyword = false)
    {

        $word = $this->getWordlistByKeyword($keyword, $isLastKeyword);

        if ( ! isset($word[0])) {
            return new Collection([]);
        }
        if ($this->fuzziness) {
            return $this->getAllDocumentsForFuzzyKeyword($word, $noLimit);
        }

        return $this->getAllDocumentsForStrictKeyword($word, $noLimit);
    }

    /**
     * @param      $keyword
     * @param bool $noLimit
     *
     * @return Collection
     */
    public function getAllDocumentsForWhereKeywordNot($keyword, $noLimit = false)
    {
        $doclistTable = $this->getTableName('doclist');

        $word = $this->getWordlistByKeyword($keyword);
        if ( ! isset($word[0])) {
            return new Collection([]);
        }
        $query = "SELECT * FROM $doclistTable WHERE doc_id NOT IN (SELECT doc_id FROM $doclistTable WHERE term_id = :id) GROUP BY doc_id ORDER BY hit_count DESC LIMIT {$this->maxDocs}";
        if ($noLimit) {
            $query = "SELECT * FROM $doclistTable WHERE doc_id NOT IN (SELECT doc_id FROM $doclistTable WHERE term_id = :id) GROUP BY doc_id ORDER BY hit_count DESC";
        }
        $stmtDoc = $this->index->prepare($query);

        $stmtDoc->bindValue(':id', $word[0]['id']);
        $stmtDoc->execute();

        return new Collection($stmtDoc->fetchAll(PDO::FETCH_ASSOC));
    }

    /**
     * @param      $keyword
     * @param bool $isLastWord
     *
     * @return int
     */
    public function totalMatchingDocuments($keyword, $isLastWord = false)
    {
        $occurance = $this->getWordlistByKeyword($keyword, $isLastWord);
        if (isset($occurance[0])) {
            return $occurance[0]['num_docs'];
        }

        return 0;
    }

    /**
     * @param      $keyword
     * @param bool $isLastWord
     *
     * @return array
     */
    public function getWordlistByKeyword($keyword, $isLastWord = false)
    {

        $wordlistTable = $this->getTableName('wordlist');

        $searchWordlist = "SELECT * FROM $wordlistTable WHERE term like :keyword LIMIT 1";
        $stmtWord       = $this->index->prepare($searchWordlist);

        if ($this->asYouType) {
            $searchWordlist = "SELECT * FROM $wordlistTable WHERE term like :keyword ORDER BY length(term) ASC, num_hits DESC LIMIT 100";
            $stmtWord       = $this->index->prepare($searchWordlist);
            $stmtWord->bindValue(':keyword', "%" . mb_strtolower($keyword) . "%");
        } else {
            $stmtWord->bindValue(':keyword', mb_strtolower($keyword));
        }
        $stmtWord->execute();
        $res = $stmtWord->fetchAll(PDO::FETCH_ASSOC);

        if ($this->fuzziness && ! isset($res[0])) {
            return $this->fuzzySearch($keyword);
        }

        return $res;
    }

    /**
     * @param $keyword
     *
     * @return array
     */
    public function fuzzySearch($keyword)
    {

        $wordlistTable = $this->getTableName('wordlist');

        $prefix         = substr($keyword, 0, $this->fuzzy_prefix_length);
        $searchWordlist = "SELECT * FROM $wordlistTable WHERE term like :keyword ORDER BY num_hits DESC LIMIT {$this->fuzzy_max_expansions}";
        $stmtWord       = $this->index->prepare($searchWordlist);
        $stmtWord->bindValue(':keyword', mb_strtolower($prefix) . "%");
        $stmtWord->execute();
        $matches = $stmtWord->fetchAll(PDO::FETCH_ASSOC);

        $resultSet = [];
        foreach ($matches as $match) {
            $distance = levenshtein($match['term'], $keyword);
            if ($distance <= $this->fuzzy_distance) {
                $match['distance'] = $distance;
                $resultSet[]       = $match;
            }
        }

        // Sort the data by distance, and than by num_hits
        $distance = [];
        $hits     = [];
        foreach ($resultSet as $key => $row) {
            $distance[$key] = $row['distance'];
            $hits[$key]     = $row['num_hits'];
        }
        array_multisort($distance, SORT_ASC, $hits, SORT_DESC, $resultSet);

        return $resultSet;
    }

    public function totalDocumentsInCollection()
    {
        return $this->getValueFromInfoTable('total_documents');
    }

    public function getStemmer()
    {
        return $this->stemmer;
    }

    public function setStemmer()
    {
        $stemmer = $this->getValueFromInfoTable('stemmer');
        if ($stemmer) {
            $this->stemmer = new $stemmer;
        } else {
            $this->stemmer = isset($this->config['stemmer']) ? new $this->config['stemmer'] : new PorterStemmer;
        }
    }

    /**
     * @return bool
     */
    public function isFileSystemIndex()
    {
        return $this->getValueFromInfoTable('driver') == 'filesystem';
    }

    public function getValueFromInfoTable($value)
    {
        $infoTable = $this->getTableName('info');

        $query = "SELECT * FROM $infoTable WHERE ikey = '$value'";
        $docs  = $this->index->query($query);

        return $docs->fetch(PDO::FETCH_ASSOC)['ivalue'];
    }

    public function filesystemMapIdsToPaths($docs)
    {
        $query = "SELECT * FROM filemap WHERE id in (" . $docs->implode(', ') . ");";
        $res   = $this->index->query($query)->fetchAll(PDO::FETCH_ASSOC);

        return $docs->map(function ($key) use ($res) {
            $index = array_search($key, array_column($res, 'id'));

            return $res[$index];
        });
    }

    public function info($str)
    {
        echo $str . "\n";
    }

    public function breakIntoTokens($text)
    {
        return $this->tokenizer->tokenize($text);
    }

    /**
     * @param        $text
     * @param        $needle
     * @param string $tag
     * @param array $options
     *
     * @return string
     */
    public function highlight($text, $needle, $tag = 'em', $options = [])
    {
        $hl = new Highlighter;

        return $hl->highlight($text, $needle, $tag, $options);
    }

    public function snippet($words, $fulltext, $rellength = 300, $prevcount = 50, $indicator = '...')
    {
        $hl = new Highlighter;

        return $hl->extractRelevant($words, $fulltext, $rellength, $prevcount, $indicator);
    }

    /**
     * @return TNTIndexer
     */
    public function getIndex()
    {
        $indexer           = new TNTIndexer;
        $indexer->inMemory = false;
        $indexer->setIndex($this->index);
        $indexer->setStemmer($this->stemmer);

        return $indexer;
    }

    /**
     * @param $words
     * @param $noLimit
     *
     * @return Collection
     */
    private function getAllDocumentsForFuzzyKeyword($words, $noLimit)
    {

        $doclistTable = $this->getTableName('doclist');

        $binding_params = implode(',', array_fill(0, count($words), '?'));
        $query          = "SELECT * FROM $doclistTable WHERE term_id in ($binding_params) ORDER BY CASE term_id";
        $order_counter  = 1;

        foreach ($words as $word) {
            $query .= " WHEN " . $word['id'] . " THEN " . $order_counter++;
        }

        $query .= " END";

        if ( ! $noLimit) {
            $query .= " LIMIT {$this->maxDocs}";
        }

        $stmtDoc = $this->index->prepare($query);

        $ids = null;
        foreach ($words as $word) {
            $ids[] = $word['id'];
        }

        $stmtDoc->execute($ids);

        return new Collection($stmtDoc->fetchAll(PDO::FETCH_ASSOC));
    }

    /**
     * @param $word
     * @param $noLimit
     *
     * @return Collection
     */
    private function getAllDocumentsForStrictKeyword($word, $noLimit)
    {

        $doclistTable = $this->getTableName('doclist');

        $query = "SELECT * FROM $doclistTable WHERE term_id = :id ORDER BY hit_count DESC LIMIT {$this->maxDocs}";
        if ($noLimit) {
            $query = "SELECT * FROM $doclistTable WHERE term_id = :id ORDER BY hit_count DESC";
        }
        $stmtDoc = $this->index->prepare($query);

        $stmtDoc->bindValue(':id', $word[0]['id']);
        $stmtDoc->execute();

        return new Collection($stmtDoc->fetchAll(PDO::FETCH_ASSOC));
    }

    /**
     * Get MySQL table name
     *
     * @return sting table type
     */
    private function getTableName($type)
    {
        global $wpdb;
        $tableName = '';

        $suffix = $this->getTableSuffix();

        switch ($type) {
            case 'wordlist':
                $tableName = $wpdb->dgwt_wcas_si_wordlist . $suffix;
                break;
            case 'doclist':
                $tableName = $wpdb->dgwt_wcas_si_doclist . $suffix;
                break;
            case 'info':
                $tableName = $wpdb->dgwt_wcas_si_info;
                break;
        }

        return $tableName;
    }

    /**
     * Get table suffix
     *
     * @return string
     */
    public function getTableSuffix()
    {
        $suffix = '';

        if ( ! empty($this->postType) && $this->postType !== 'product') {
            $suffix .= '_' . $this->postType;
        }

        if ( ! empty($this->lang)) {
            $suffix .= '_' . $this->lang;
        }

        return $suffix;
    }
}
