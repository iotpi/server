<?php

declare(strict_types=1);
/**
 * @copyright Copyright (c) 2020, Morris Jobke <hey@morrisjobke.de>
 *
 * @author Morris Jobke <hey@morrisjobke.de>
 *
 * @license GNU AGPL version 3 or any later version
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 */

namespace OC\Core\Command\Maintenance;

use bantu\IniGetWrapper\IniGetWrapper;
use OC\Files\Node\File;
use OCP\DB\QueryBuilder\IQueryBuilder;
use OCP\Files\Folder;
use OCP\Files\IRootFolder;
use OCP\Files\NotFoundException;
use OCP\IConfig;
use OCP\IDBConnection;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Question\ConfirmationQuestion;

class RepairMultibucketPreviews extends Command {
	/** @var IConfig */
	protected $config;
	/** @var IDBConnection */
	private $db;
	/**
	 * @var IRootFolder
	 */
	private $rootFolder;
	/** @var bool */
	private $stopSignalReceived = false;
	/** @var IniGetWrapper */
	private $phpIni;

	public function __construct(IConfig $config, IDBConnection $db, IRootFolder $rootFolder, IniGetWrapper $phpIni) {
		$this->config = $config;
		$this->db = $db;
		parent::__construct();
		$this->rootFolder = $rootFolder;

		pcntl_signal(SIGINT, [$this, 'sigIntHandler']);
		$this->phpIni = $phpIni;
	}

	protected function configure() {
		$this
			->setName('maintenance:repair:multibucketpreviews')
			->setDescription('moves the previews in a multibucket object store setup into multiple buckets');
	}

	protected function execute(InputInterface $input, OutputInterface $output): int {
		$mountPoint = $this->rootFolder->getMountPoint();
		$storageId = $mountPoint->getStorageId();
		$numericStorageId = $mountPoint->getNumericStorageId();

		$instanceId = $this->config->getSystemValueString('instanceid');

		$output->writeln("This will migrate all previews from the root storage with the ID: $storageId ($numericStorageId)");
		$output->writeln('');

		$query = $this->db->getQueryBuilder();
		$mimetype = $query->select('id')
			->from('mimetypes')
			->where($query->expr()->eq('mimetype', $query->createNamedParameter('httpd/unix-directory')))
			->execute()
			->fetchColumn(0);

		$query = $this->db->getQueryBuilder();
		$query->select('fileid', 'path', 'parent')
			->from('filecache')
			->where($query->expr()->eq('storage', $query->createNamedParameter($numericStorageId, IQueryBuilder::PARAM_INT)))
			->andWhere($query->expr()->like('path', $query->createNamedParameter("appdata_$instanceId/preview/%")))
			->andWhere($query->expr()->neq('mimetype', $query->createNamedParameter($mimetype, IQueryBuilder::PARAM_INT)))
			->orderBy('parent');

		$output->writeln('Fetching previews that need to be migrated …');
		$result = $query->execute();

		$rows = $result->fetchAll();
		$total = count($rows);
		$fileIds = [];
		foreach ($rows as $row) {
			$fileIds[$row['parent']] = true;
		}
		$totalFileIds = count($fileIds);

		if ($total === 0) {
			$output->writeln("All previews are already migrated.");
			return 0;
		}

		$output->writeln("A total of $total preview files need to be migrated. Those are generated from $totalFileIds actual files.");
		$output->writeln("");
		$output->writeln("The migration will always migrate all previews of a single file in a batch. After each batch the process can be canceled by pressing CTRL-C. This fill finish the current batch and then stop the migration. This migration can then just be started and it will continue.");

		$helper = $this->getHelper('question');
		$question = new ConfirmationQuestion('<info>Should the migration be started? (y/[n]) </info>', false);

		if (!$helper->ask($input, $output, $question)) {
			return 0;
		}

		$output->writeln("");
		$output->writeln("");
		$progressBar = new ProgressBar($output, $totalFileIds);
		$progressBar->setFormat("%current%/%max% [%bar%] %percent:3s%% %elapsed:6s%/%estimated:-6s% Memory: %memory:6s% \n %message%");
		$progressBar->setMessage("Starting …");
		$progressBar->start();

		$lastParentPath = dirname($rows[0]['path']);
		$oldParent = null;
		foreach ($rows as $row) {
			pcntl_signal_dispatch();
			$currentParentPath = dirname($row['path']);
			if ($currentParentPath !== $lastParentPath) {
				$progressBar->setMessage("Deleting empty parent folders …");
				$progressBar->display();
				do {
					$newParent = $oldParent->getParent();
					$oldParent->delete();
					$oldParent = $newParent;
					$childs = $oldParent->getDirectoryListing();
				} while (!isset($childs[0]));

				if ($this->stopSignalReceived) {
					$progressBar->setMessage("Reached end of a batch and stopping.");
					return 0;
				}

				$memoryLimit = $this->phpIni->getBytes('memory_limit');
				$memoryUsage = memory_get_usage();
				if ($memoryLimit - $memoryUsage < 25 * 1024 * 1024) {
					$output->writeln("");
					$output->writeln("");
					$output->writeln("");
					$output->writeln("Stopped process 25 MB before reaching the memory limit to avoid a hard crash.");
					$memoryLimit = round($memoryLimit/1024/1024, 0);
					$memoryUsage = round($memoryUsage/1024/1024, 0);
					$output->writeln("Memory limit: $memoryLimit MB Memory usage: $memoryUsage MB");
					$progressBar->setMessage("Reached memory limit and stopped to avoid hard crash.");
					return 1;
				}

				$lastParentPath = $currentParentPath;
				$progressBar->advance();
			}

			$oldPath = str_replace('/preview/', '/preview/old-multibucket/', $row['path']);
			$newPath = $row['path'];
			$progressBar->setMessage("Moving $oldPath to $newPath");
			$progressBar->display();

			/** @var File $oldNode */
			$oldNode = $this->rootFolder->get($oldPath);

			$newFoldername = dirname($newPath);
			try {
				$this->rootFolder->get($newFoldername);
			} catch (NotFoundException $e) {
				$this->rootFolder->newFolder($newFoldername);
			}
			$oldNode->move($newPath);

			$oldParent = $oldNode->getParent();
		}

		$progressBar->setMessage("Deleting empty parent folders …");
		$progressBar->display();
		do {
			$newParent = $oldParent->getParent();
			$oldParent->delete();
			$oldParent = $newParent;
			$childs = $oldParent->getDirectoryListing();
		} while (!isset($childs[0]));

		$progressBar->finish();

		$output->writeln("");

		return 0;
	}

	protected function sigIntHandler() {
		echo "\n\nSignal received - will finish the step and then stop the migration.\n\n";
		$this->stopSignalReceived = true;
	}
}
