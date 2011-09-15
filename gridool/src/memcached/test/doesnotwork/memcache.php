<?php

ini_set('memcache.protocol', 'binary');

$memcache = new Memcache;
$memcache->connect('192.168.142.129', 11211) or die ("connect failed");

$tmp_object = new stdClass;
$tmp_object->str_attr = 'test';
$tmp_object->int_attr = 123;

$memcache->set('key', $tmp_object) or die ("set failed");

$get_result = $memcache->get('key');
var_dump($get_result);

$get_result = $memcache->get('key');
var_dump($get_result);

?>