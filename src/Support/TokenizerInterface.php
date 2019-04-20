<?php
namespace TeamTNT\TNTSearchASFW\Support;

interface TokenizerInterface
{
    public function tokenize($text, $stopwords);
}
