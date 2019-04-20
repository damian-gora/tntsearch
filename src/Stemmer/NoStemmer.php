<?php

namespace TeamTNT\TNTSearchASFW\Stemmer;

class NoStemmer implements Stemmer
{
    public static function stem($word)
    {
        return $word;
    }
}