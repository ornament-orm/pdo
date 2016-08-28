<?php

namespace Ornament\Pdo;

use Ornament\Ornament\DefaultAdapter;
use Ornament\Container;
use Ornament\Exception;
use PDOException;
use zpt\anno\Annotations;
use zpt\anno\AnnotationParser;

/**
 * Ornament adapter for PDO data sources.
 */
class Adapter extends DefaultAdapter
{
    /** @var array Private statement cache. */
    private $statements = [];

    /** @var array Additional query parameters. */
    protected $parameters = [];

    /**
     * Constructor. Pass in the adapter (instanceof PDO) as an argument.
     *
     * @param PDO $adapter
     * @param string $id Optional "id" (table name)
     * @param array $props Optional array of property names this adapter
     *  instance works on.
     * @return void
     */
    public function __construct(\PDO $adapter, $id = null, array $props = null)
    {
        $this->adapter = $adapter;
        if (isset($id)) {
            $this->setIdentifier($id);
        }
        if (isset($props)) {
            $this->setProperties($props);
        }
    }

    public function setAdditionalQueryParameters(array $parameters)
    {
        $this->parameters = $parameters;
    }

    /**
     * Query $object (a model) using $parameters with optional $opts.
     *
     * @param object $object A model object.
     * @param array $parameters Key/value pair or WHERE statements, e.g.
     *  ['id' => 1].
     * @param array $opts Hash of options. Supported keys are 'limit',
     *  'offset' and 'order' and they correspond to their SQL equivalents.
     * @param array $ctor Optional constructor arguments.
     * @return array|false An array of objects of the same class as $object, or
     *  false on query failure.
     */
    public function query($object, array $parameters, array $opts = [], array $ctor = [])
    {
        $keys = [];
        $values = $this->parameters;
        $identifier = $this->identifier;
        foreach ($parameters as $key => $value) {
            if (!strpos($key, '.')) {
                $key = "$identifier.$key";
            }
            $keys[$key] = sprintf('%s = ?', $key);
            $values[] = $value;
        }
        $fields = $this->fields;
        foreach ($fields as &$field) {
            $field = "$identifier.$field";
        }
        $identifier .= $this->generateJoin($fields);
        $sql = "SELECT %s FROM %s WHERE %s";
        $sql = sprintf(
            $sql,
            implode(', ', $fields),
            $identifier,
            $keys ?  implode(' AND ', $keys) : '(1 = 1)'
        );
        if (isset($opts['order'])) {
            $sql .= sprintf(
                ' ORDER BY %s',
                preg_replace('@[^\w,\s\(\)]', '', $opts['order'])
            );
        }
        if (isset($opts['limit'])) {
            $sql .= sprintf(' LIMIT %d', $opts['limit']);
        }
        if (isset($opts['offset'])) {
            $sql .= sprintf(' OFFSET %d', $opts['offset']);
        }
        $stmt = $this->getStatement($sql);
        try {
            $stmt->execute($values);
            $stmt->setFetchMode(Base::FETCH_CLASS, get_class($object), $ctor);
            return $stmt->fetchAll();
        } catch (PDOException $e) {
            return false;
        }
    }

    /**
     * Load data into a single model.
     *
     * @param Container $object A container object.
     * @return void
     * @throws Ornament\Exception\PrimaryKey if no primary key was set or could
     *  be determined, and loading would inevitably fail.
     */
    public function load(Container $object)
    {
        $pks = [];
        $values = $this->parameters;
        $identifier = $this->identifier;
        foreach ($this->primaryKey as $key) {
            if (isset($object->$key)) {
                $pks[$key] = sprintf('%s.%s = ?', $identifier, $key);
                $values[] = $object->$key;
            } else {
                throw new Exception\PrimaryKey($identifier, $key);
            }
        }
        $fields = $this->fields;
        foreach ($fields as &$field) {
            $field = "$identifier.$field";
        }
        $identifier .= $this->generateJoin($fields);
        $sql = "SELECT %s FROM %s WHERE %s";
        $stmt = $this->getStatement(sprintf(
            $sql,
            implode(', ', $fields),
            $identifier,
            implode(' AND ', $pks)
        ));
        $stmt->setFetchMode(Base::FETCH_INTO, $object);
        $stmt->execute($values);
        $stmt->fetch();
        $object->markClean();
    }

    /**
     * Internal helper to generate a JOIN statement.
     *
     * @param array $fields Array of fields to extend.
     * @return string The JOIN statement to append to the query string.
     */
    protected function generateJoin(array &$fields)
    {
        $annotations = $this->annotations['class'];
        $props = $this->annotations['properties'];
        $table = '';
        foreach (['Require' => '', 'Include' => 'LEFT '] as $type => $join) {
            if (isset($annotations[$type])) {
                foreach ($annotations[$type] as $local => $joinCond) {
                    if (is_numeric($local)) {
                        foreach ($joinCond as $local => $joinCond) {
                        }
                    }
                    // Hack to make the annotationParser recurse.
                    $joinCond = AnnotationParser::getAnnotations(
                        '/** @joinCond '.implode(', ', $joinCond).' */'
                    )['joincond'];
                    $table .= sprintf(
                        ' %1$sJOIN %2$s ON ',
                        $join,
                        $local
                    );
                    $conds = [];
                    foreach ($joinCond as $ref => $me) {
                        if ($me == '?') {
                            $conds[] = sprintf(
                                "%s.%s = $me",
                                $local,
                                $ref
                            );
                        } else {
                            $conds[] = sprintf(
                                "%s.%s = %s.%s",
                                $local,
                                $ref,
                                $this->identifier,
                                $me
                            );
                        }
                    }
                    $table .= implode(" AND ", $conds);
                }
            }
        }
        foreach ($fields as &$field) {
            $name = str_replace("{$this->identifier}.", '', $field);
            if (isset($props[$name]['From'])) {
                $field = sprintf(
                    '%s %s',
                    $props[$name]['From'],
                    $name
                );
            }
        }
        return $table;
    }

    /**
     * Protected helper to either get or create a PDOStatement.
     *
     * @param string $sql SQL to prepare the statement with.
     * @return PDOStatement A PDOStatement.
     */
    protected function getStatement($sql)
    {
        if (!isset($this->statements[$sql])) {
            $this->statements[$sql] = $this->adapter->prepare($sql);
        }
        return $this->statements[$sql];
    }

    /**
     * Persist the newly created Container $object.
     *
     * @param Ornament\Container $object The model to persist.
     * @return boolean True on success, else false.
     */
    public function create(Container $object)
    {
        $sql = "INSERT INTO %1\$s (%2\$s) VALUES (%3\$s)";
        $placeholders = [];
        $values = [];
        foreach ($this->fields as $field) {
            if (property_exists($object, $field)
                && !isset($this->annotations['properties'][$field]['From'])
                && isset($object->$field)
            ) {
                $placeholders[$field] = '?';
                $values[] = $object->$field;
            }
        }
        $this->flattenValues($values);
        $sql = sprintf(
            $sql,
            $this->identifier,
            implode(', ', array_keys($placeholders)),
            implode(', ', $placeholders)
        );
        $stmt = $this->getStatement($sql);
        try {
            $retval = $stmt->execute($values);
        } catch (PDOException $e) {
            return false;
        }
        if (count($this->primaryKey) == 1) {
            $pk = $this->primaryKey[0];
            try {
                $object->$pk = $this->adapter->lastInsertId($this->identifier);
                $this->load($object);
            } catch (PDOException $e) {
                // Means this is not supported by this engine.
            }
        }
        return $retval;
    }

    /**
     * Persist the existing Container $object back to the RDBMS.
     *
     * @param Ornament\Container $object The model to persist.
     * @return boolean True on success, else false.
     */
    public function update(Container $object)
    {
        $sql = "UPDATE %1\$s SET %2\$s WHERE %3\$s";
        $placeholders = [];
        $values = [];
        foreach ($this->fields as $field) {
            if (property_exists($object, $field)
                && !isset($this->annotations['properties'][$field]['From'])
            ) {
                if (!is_null($object->$field)) {
                    $placeholders[$field] = sprintf('%s = ?', $field);
                    $values[] = $object->$field;
                } else {
                    $placeholders[$field] = "$field = NULL";
                }
            }
        }
        $this->flattenValues($values);
        $primaries = [];
        foreach ($this->primaryKey as $key) {
            $primaries[] = sprintf('%s = ?', $key);
            $values[] = $object->$key;
        }
        $sql = sprintf(
            $sql,
            $this->identifier,
            implode(', ', $placeholders),
            implode(' AND ', $primaries)
        );
        $stmt = $this->getStatement($sql);
        try {
            $retval = $stmt->execute($values);
            $this->load($object);
            return $retval;
        } catch (PDOException $e) {
            return false;
        }
    }

    /**
     * Delete the existing Container $object from the RDBMS.
     *
     * @param Ornament\Container $object The model to delete.
     * @return boolean True on success, else false.
     */
    public function delete(Container $object)
    {
        $sql = "DELETE FROM %1\$s WHERE %2\$s";
        $primaries = [];
        foreach ($this->primaryKey as $key) {
            $primaries[] = sprintf('%s = ?', $key);
            $values[] = $object->$key;
        }
        $sql = sprintf(
            $sql,
            $this->identifier,
            implode(' AND ', $primaries)
        );
        $stmt = $this->getStatement($sql);
        try {
            $retval = $stmt->execute($values);
            return $retval;
        } catch (PDOException $e) {
            return false;
        }
    }
}

