<?php

declare(strict_types=1);

/**
 * @copyright 2020 Christoph Wurst <christoph@winzerhof-wurst.at>
 *
 * @author 2020 Christoph Wurst <christoph@winzerhof-wurst.at>
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
 */

namespace OCP\Search;

use JsonSerializable;

/**
 * Represents an entry in a list of results an app returns for a unified search
 * query.
 *
 * The app providing the results has to extend this class for customization. In
 * most cases apps do not have to add any additional code.
 *
 * @example ``class MailResultEntry extends ASearchResultEntry {}`
 *
 * This approach was chosen over a final class as it allows Nextcloud to later
 * add new optional properties of an entry without having to break the usage of
 * this class in apps.
 *
 * @since 20.0.0
 */
abstract class ASearchResultEntry implements JsonSerializable {

	/**
	 * @var string
	 * @since 20.0.0
	 */
	protected $thumbnailUrl;

	/**
	 * @var string
	 * @since 20.0.0
	 */
	protected $title;

	/**
	 * @var string
	 * @since 20.0.0
	 */
	protected $subline;

	/**
	 * @var string
	 * @since 20.0.0
	 */
	protected $resourceUrl;

	/**
	 * @var string
	 * @since 20.0.0
	 */
	protected $iconClass;

	/**
	 * @var boolean
	 * @since 20.0.0
	 */
	protected $rounded;

	/**
	 * @param string $thumbnailUrl a relative or absolute URL to the thumbnail or icon of the entry
	 * @param string $title a main title of the entry
	 * @param string $subline the secondary line of the entry
	 * @param string $resourceUrl the URL where the user can find the detail, like a deep link inside the app
	 * @param string $iconClass the icon class fallback
	 * @param boolean $rounded is the thumbnail rounded
	 *
	 * @since 20.0.0
	 */
	public function __construct(string $thumbnailUrl,
								string $title,
								string $subline,
								string $resourceUrl,
								string $iconClass = '',
								bool $rounded = false) {
		$this->thumbnailUrl = $thumbnailUrl;
		$this->title = $title;
		$this->subline = $subline;
		$this->resourceUrl = $resourceUrl;
		$this->iconClass = $iconClass;
		$this->rounded = $rounded;
	}

	/**
	 * @return array
	 *
	 * @since 20.0.0
	 */
	public function jsonSerialize(): array {
		return [
			'thumbnailUrl' => $this->thumbnailUrl,
			'title' => $this->title,
			'subline' => $this->subline,
			'resourceUrl' => $this->resourceUrl,
			'iconClass' => $this->iconClass,
			'rounded' => $this->rounded,
		];
	}
}
