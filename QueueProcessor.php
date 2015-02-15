<?php
/*
 * @package    QueueProcessor
 * @version    1.0.0
 * @author     Roman Skritskiy <romanskritskiy@gmail.com>
 */
abstract class QueueProcessor {

    private $_queue_data_dir     = 'queue_data';
    private $_status_file        = 'status.json';
    private $_queue_file         = 'queue.json';
    private $_queue_results_file = 'queue_results.json';

    private $max_exec_time;
    private $max_nesting         = 40;

    // Default delay time between retries of processing in microseconds (0.5 second by default).
    private $delay               = 500000;

    protected $queue             = [];
    protected $queue_name;
    protected $class_root;
    protected $start_time;
    protected $recursion_count;


    public function __construct() {
        // Set overall max execution time in seconds according to current PHP setting value.
        $this->setMaxExecTime();

        // Set maximum number of recursion calls of request execution callback.
        // By default, 40 retries with 0.5 second delay will give around 20 secs of total execution time.
        // That's probably enough for the average webserver.
        $this->setMaxNesting();

        // Define & create a directory for QueueProcessor's system files within a specified by child class directory.
        $this->_queue_data_dir = $this->_getQueueStaticDataDirRoot() . "/{$this->_queue_data_dir}/";
        if (!is_dir($this->_queue_data_dir) && !file_exists($this->_queue_data_dir)) {
            if (!mkdir($this->_queue_data_dir)) {
                throw new Exception("Unable to create QueueProcessor's system directory.");
            }
        }

        // Create QueueProcessor files (if they're not exist yet) and store their full paths.
        $this->_status_file          = $this->createFile($this->_queue_data_dir, $this->_status_file, '{"locked":0}');
        $this->_queue_file           = $this->createFile($this->_queue_data_dir, $this->_queue_file, '{}');
        $this->_queue_results_file   = $this->createFile($this->_queue_data_dir, $this->_queue_results_file, '{}');

        if (!$this->_status_file || !$this->_queue_file || !$this->_queue_results_file) {
            throw new Exception("Unable to create QueueProcessor's system files.");
        }
    }


    /*
     * Get a directory's full path where 'queue_data' directory with all QueueProcessor's system files will be stored.
     * This need to be implemented in child classes.
     *
     * @return string
     */
    abstract protected function _getQueueStaticDataDirRoot();


    /*
     * Process a single queue item.
     * This need to be implemented in child classes.
     *
     * @param mixed $data Data of a single queue item that need to be precessed.
     * @return string
     */
    abstract protected function _processRequest($data);


    /*
     * Set s queue element name.
     *
     * @param string $queue_name
     */
    public function setQueueName($queue_name) {
        $this->queue_name = (string) $queue_name;

        return $this;
    }


    /*
     * Internal recursive processing callback of a queue item.
     */
    protected function _executeRequestInternal($data = array()) {
        // Get start time of execution (first iteration on potential recursive cycle).
        $this->start_time = $this->start_time ? : microtime(TRUE);

        $this->recursion_count++;

        $result = array(
            'success'        => FALSE,
            'request_result' => NULL,
            'system'         => array(
                'msg'        => '',
                'queue_name' => NULL
            ),
        );


        // Break execution if we reached max nesting calls (recursion) limit.
        if ($this->recursion_count > $this->getMaxNesting()) {
            $result['system']['msg'] = 'timeout';

            return $result;
        }


        // Get status data (about "lock" for requests processing).
        $status = $this->getStatus();

        // Process request if currently no other requests are in work.
        if (empty($status['locked'])) {
            // Set lock (to prevent other requests execution).
            $this->setExecLock();

            // Define flag of change of queue list
            $isQueueUpdated = FALSE;

            // Process current data (first request).
            if (!empty($data)) {
                try {
                    $result['request_result'] = $this->_processRequest($data);
                    $result['success']        = TRUE;
                } catch (Exception $e) {
                    $result['system']['msg'] = $e->getMessage();
                }
            }
            // Otherwise, process queue list.
            else {
                // We have nothing to do if both $data and queue_name are empty.
                if ($this->queue_name) {
                    // Get fresh queue list file data.
                    $queue = $this->getQueue(TRUE);

                    // Process queue list if this request's queue item yet is in the list.
                    if (array_key_exists($this->queue_name, $queue)) {
                        // Logger of each cycle duration
                        $cycle_time_log = array();

                        foreach ($queue as $queue_name => $queue_data) {
                            $cycle_start_time = empty($cycle_time_log) ? $this->start_time : microtime(TRUE);

                            // Terminate processing if 'max_exec_time' is reached or soon to be (according to average cycle time).
                            $time_eplased   = ($cycle_start_time - $this->start_time);
                            $time_predicted = ($time_eplased + (array_sum($cycle_time_log) / count($cycle_time_log)));
                            if ($time_eplased >= $this->getMaxExecTime() || $time_predicted > $this->getMaxExecTime()) {
                                $result['system']['msg']        = 'timeout';
                                $result['system']['queue_name'] = $this->queue_name;
                                break;
                            }

                            // Actual processing of the request.
                            try {
                                $_result_raw       = $this->_processRequest($data);
                                $result['success'] = TRUE;
                            } catch (Exception $e) {
                                $result['system']['msg'] = $e->getMessage();
                            }

                            // Delete queue item from list.
                            unset($queue[$queue_name]);

                            // Break out, if this request's queue item was processed.
                            if ($queue_name == $this->queue_name) {
                                $result['request_result'] = $_result_raw;
                                break;
                            }
                            // Log the result of queue item processing in cache queue results file.
                            else {
                                $this->addRequestProcessingResultToLog($this->queue_name, $_result_raw);
                            }

                            // Log duration of this iteration and set "change flag" of queue list
                            $cycle_time_log[] = (microtime(TRUE) - $cycle_start_time);
                            $isQueueUpdated   = TRUE;
                        }
                        unset($queue_name, $queue_data, $cycle_time_log, $cycle_start_time, $time_eplased, $time_predicted);

                        // Set updated queue data.
                        $this->setData('queue', $queue);
                    }
                    // Get result of processing of this task (queue item) from "queue results" file.
                    else {
                        $result['request_result'] = $this->getRequestProcessingResultFromLog($this->queue_name);
                    }
                }
                else {
                    $result['system_message'] = 'no_data';
                }
            }

            // Update queue list.
            if ($isQueueUpdated) {
                $this->updateQueueList();
            }

            // Remove lock
            $this->removeExecLock();
        }
        // Otherwise, add data to queue list and try to process the queue list.
        else {
            $shouldRunAgain = FALSE;

            // Add data to the queue list if this is a first attempt.
            if (!empty($data) && empty($this->queue_name)) {
                $this->addQueue($data);
                $shouldRunAgain = TRUE;
            }
            // Otherwise, check if there is a result of processing of this queue item in "queue results" file
            else {
                $result['request_result'] = $this->getRequestProcessingResultFromLog($this->queue_name);
                if (empty($result['request_result'])) {
                    $shouldRunAgain = TRUE;
                } else {
                    $result['success'] = TRUE;
                }
            }

            // Run this callback again if this is a first attempt or no results yet.
            if ($shouldRunAgain) {
                // Wait before next try.
                usleep($this->delay);

                // Process queue list
                $result = $this->_executeRequestInternal();
            }
        }

        return $result;
    }


    /*
     * Get queue data
     *
     * @param   bool    $getFresh   Flag to get data from queue file
     * @return  array
     */
    protected function getQueue($getFresh = FALSE, $queue_file_handle = NULL) {
        if ($getFresh) {
            $queue = (is_null($queue_file_handle) ? file_get_contents($this->_queue_file) : stream_get_contents($queue_file_handle));
            return json_decode($queue, TRUE);
        }

        if (empty($this->queue)) {
            $this->queue = json_decode(file_get_contents($this->_queue_file), TRUE);
        }

        return !empty($this->queue) ? $this->queue : array();
    }


    /*
     * Add new queue item to queue file
     */
    protected function addQueue($data) {
        // Open queue file
        $queue_file_handle = fopen($this->_queue_file, "r+");

        // Check queue file is writable (not locked for writing by other request)
        if (flock($queue_file_handle, LOCK_EX)) {
            // Get queue data from file
            $queue = $this->getQueue(TRUE, $queue_file_handle);

            // Generate a unique name for a new queue item
            $queue_name = $this->generateQueueName();
            while (array_key_exists($queue_name, $queue)) {
                $queue_name = $this->generateQueueName();
            }

            // Add queue item to queue && write updated data to file
            $queue[$queue_name] = $data;
            $result = file_put_contents($this->_queue_file, json_encode($queue));

            // Remove file write lock
            flock($queue_file_handle, LOCK_UN);
        } else {
            $result = FALSE;
        }

        return $result;
    }


    /*
     * Update queue list: merge updated processed queue with that currently stored in file.
     */
    protected function updateQueueList() {
        return $this->mergeJsonFileObjectData($this->_queue_file, $this->getQueue());
    }


    /*
     * Get status data
     *
     * @return  array
     */
    protected function getStatus($file_handle = NULL) {
        $status_data = (is_null($file_handle) ? file_get_contents($this->_status_file) : stream_get_contents($file_handle));
        return json_decode($status_data, TRUE);
    }


    /*
     * Set lock to prevent any parallel request processing
     */
    protected function setExecLock() {
        $file_handle = fopen($this->_status_file, "r+");

        // Check status file is writable (not locked for writing by other request).
        // If not, flock() will wait until the other request is finished and its lock will be removed.
        if (flock($file_handle, LOCK_EX)) {
            $status_data = $this->getStatus($file_handle);
            $status_data['locked'] = 1;

            $result = file_put_contents($this->_status_file, json_encode($status_data));

            // Remove file write lock
            flock($file_handle, LOCK_UN);
        } else {
            $result = FALSE;
        }

        fclose($file_handle);

        return (!empty($result));
    }


    /*
     * Remove lock to allow a new request to be processed
     */
    protected function removeExecLock() {
        $file_handle = fopen($this->_status_file, "r+");

        // Check status file is writable (not locked for writing by other request).
        // If not, flock() will wait until the other request is finished and its lock will be removed.
        if (flock($file_handle, LOCK_EX)) {
            $status_data = $this->getStatus($file_handle);
            $status_data['locked'] = 0;

            $result = file_put_contents($this->_status_file, json_encode($status_data));

            // Remove file write lock
            flock($file_handle, LOCK_UN);
        } else {
            $result = FALSE;
        }

        fclose($file_handle);

        return (!empty($result));
    }


    /*
     * Get result of processing of queue item from log file
     */
    protected function getRequestProcessingResultFromLog($queue_name) {
        $result = array();

        if (!empty($queue_name)) {
            $queue_results = json_decode(file_get_contents($this->_queue_results_file), TRUE);

            if (!empty($queue_results[$queue_name])) {
                $result = $queue_results[$queue_name];

                // Remove this result from log.
                unset($queue_results[$queue_name]);
                $this->setRequestProcessingResultToLog($queue_results);
            }
        }

        return $result;
    }


    /*
     * Add result of processing of queue item to queue results log file.
     */
    protected function addRequestProcessingResultToLog($queue_name, $data) {
        // Open file
        $queue_file_handle = fopen($this->_queue_results_file, "r+");

        // Check file is writable (not locked for writing by other request)
        if (flock($queue_file_handle, LOCK_EX)) {
            // Get queue results data from file
            $queue_results = json_decode(file_get_contents($this->_queue_results_file), TRUE);

            // Add queue item to queue
            $queue_results[$queue_name] = $data;

            // Write updated data to file
            $result = file_put_contents($this->_queue_results_file, json_encode($queue_results));

            // Remove file write lock
            flock($queue_file_handle, LOCK_UN);
        } else {
            $result = FALSE;
        }

        return (!empty($result));
    }


    /*
     * Merge results of processing of queue items to queue results log file.
     */
    protected function setRequestProcessingResultToLog($queue_results_updated) {
        return $this->mergeJsonFileObjectData($this->_queue_results_file, $queue_results_updated);
    }


    /*
     * Set data for class' properties
     *
     * @param   string  $name   Property's name
     * @param   mixed   $value  Property's value
     * @return  $this
     */
    protected function setData($name, $value) {
        if ($name && $value) {
            $this->{$name} = $value;
        }

        return $this;
    }


    /*
     * Helper function:
     * Generate unique queue item name (as integer) and store it in class' property
     *
     * @param   int $digits_number  Number of digits for name to consist of
     * @return  int Integer queue name
     */
    protected function generateQueueName($digits_number = 4) {
        $this->queue_name = substr(number_format(time() * rand(),0,'',''), 0, $digits_number);

        return $this->queue_name;
    }


    /*
     * Helper function:
     * Create file if it's not exist yet.
     *
     * @param string $dir_name Directory full path where file should be created.
     * @param string $file_name File name.
     * @param string $file_data Data to write to created file.
     *
     * @return string Full path to the file upon success or empty string upon failure.
     */
    protected function createFile($dir_name, $file_name, $file_data = '') {
        $result = FALSE;

        if (!empty($dir_name) && !empty($file_name)) {
            $full_name = $dir_name . $file_name;
            $result = (!file_exists($full_name)) ? file_put_contents($full_name, $file_data) : TRUE;
        }

        return $result ? $full_name : '';
    }


    /*
     * Helper function:
     * Merge object data in JSON file.
     */
    protected function mergeJsonFileObjectData($file_name, $new_data, $file_data = NULL) {
        // Open file
        $queue_file_handle = fopen($file_name, "r+");

        // Check if file is writable (not locked for writing by other request)
        if (flock($queue_file_handle, LOCK_EX)) {
            // Build new merged data: toss away old data that are still in file (i.e. already processed queue items) -
            // all items that are above first item in $new_data.
            if (empty($new_data)) {
                $queue_new = array();
            } else {
                if (is_null($file_data)) {
                    $file_data = json_decode(file_get_contents($this->$file_name), TRUE);
                }

                reset($new_data);
                $first_key                 = key($new_data);
                $first_key_offset_in_fresh = array_search($first_key, array_keys($file_data), TRUE);
                $queue_new                 = array_slice($file_data, $first_key_offset_in_fresh, NULL, TRUE);
            }

            // Write updated data to file
            $result = file_put_contents($file_name, json_encode($queue_new));

            // Remove file write lock
            flock($queue_file_handle, LOCK_UN);
        } else {
            $result = FALSE;
        }

        return (!empty($result));
    }


    /**
     * Set maximum time of overall script execution
     *
     * Sets a given number of second -10 sec. to ensure all 'system' work wil get finished in time.
     * @param int $sec
     */
    protected function setMaxExecTime($sec = 0) {
        $sec = (int) $sec;
        $max = ini_get('max_execution_time');
        $sec = ($sec > 0 && $sec <= $max) ? $sec : $max;

        $this->max_exec_time = ($sec - 10);
    }

    protected function getMaxExecTime() {
        return $this->max_exec_time;
    }


    /**
     * Set maximum of nesting calls of request execution callback.
     *
     * @param int $value
     */
    protected function setMaxNesting($value = 0) {
        $max_nesting = 0;
        $xdebug_mnl = (int) ini_get('xdebug.max_nesting_level');

        $this->max_nesting = ($xdebug_mnl && $this->max_nesting < $xdebug_mnl) ? $this->max_nesting : ($xdebug_mnl - 1);
    }

    protected function getMaxNesting() {
        return $this->max_nesting;
    }


    /**
     * Set time of delay between retries (in microseconds).
     * @param int $microseconds Delay value. Default to 500000 microseconds = 0.5 second.
     */
    protected function setRetriesDelay($microseconds = 0) {
        $microseconds = (int) $microseconds;
        $this->delay = ($microseconds > 0) ? $microseconds : $this->delay;
    }
}