<?php

declare(strict_types=1);
/**
 * @copyright Copyright (c) 2020 Joas Schilling <coding@schilljs.com>
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

namespace OCA\Settings\Search;

use OCP\IGroupManager;
use OCP\IL10N;
use OCP\IURLGenerator;
use OCP\IUser;
use OCP\Search\IProvider;
use OCP\Search\ISearchQuery;
use OCP\Search\SearchResult;
use OCP\Settings\IIconSection;
use OCP\Settings\ISection;
use OCP\Settings\IManager;

class SectionSearch implements IProvider {

	/** @var IManager */
	protected $settingsManager;
	/** @var IGroupManager */
	protected $groupManager;
	/** @var IURLGenerator */
	protected $urlGenerator;
	/** @var IL10N */
	protected $l;

	public function __construct(IManager $settingsManager,
								IGroupManager $groupManager,
								IURLGenerator $urlGenerator,
								IL10N $l) {
		$this->settingsManager = $settingsManager;
		$this->groupManager = $groupManager;
		$this->urlGenerator = $urlGenerator;
		$this->l = $l;
	}

	/**
	 * @inheritDoc
	 */
	public function getId(): string {
		return 'settings_sections';
	}

	/**
	 * @inheritDoc
	 */
	public function getName(): string {
		return $this->l->t('Settings');
	}

	/**
	 * @inheritDoc
	 */
	public function search(IUser $user, ISearchQuery $query): SearchResult {
		$isAdmin = $this->groupManager->isAdmin($user->getUID());

		$result = $this->searchSections(
			$query,
			$this->settingsManager->getPersonalSections(),
			$isAdmin ? $this->l->t('Personal') : '',
			'settings.PersonalSettings.index'
		);

		if ($this->groupManager->isAdmin($user->getUID())) {
			$result = array_merge($result, $this->searchSections(
				$query,
				$this->settingsManager->getAdminSections(),
				$this->l->t('Administration'),
				'settings.AdminSettings.index'
			));
		}

		return SearchResult::complete(
			$this->l->t('Settings'),
			$result
		);
	}

	/**
	 * @param ISearchQuery $query
	 * @param ISection[][] $sections
	 * @param string $subline
	 * @param string $routeName
	 * @return array
	 */
	public function searchSections(ISearchQuery $query, array $sections, string $subline, string $routeName): array {
		$result = [];
		foreach ($sections as $priority => $sectionsByPriority) {
			foreach ($sectionsByPriority as $section) {
				if (
					stripos($section->getName(), $query->getTerm()) === false &&
					stripos($section->getID(), $query->getTerm()) === false
				) {
					continue;
				}

				$iconUrl = '';
				if ($section instanceof IIconSection) {
					$iconUrl = $section->getIcon();
				}

				$result[] = new SectionResult(
					$iconUrl,
					$section->getName(),
					$subline,
					$this->urlGenerator->linkToRouteAbsolute($routeName, ['section' => $section->getID()])
				);
			}
		}

		return $result;
	}
}
