CREATE DATABASE IF NOT EXISTS PTTDB;

CREATE TABLE IF NOT EXISTS `PTTDB`.`PTT_ARTICLE` (
  `title` VARCHAR(60) NOT NULL COMMENT '文章標題',
  `boardName` VARCHAR(40) COMMENT '看板名稱',
  `articleId` VARCHAR(45) NOT NULL COMMENT '文章編號',
  `authorId` VARCHAR(40) NOT NULL,
  `authorName` VARCHAR(45) NULL DEFAULT NULL,
  `publishedTime` TIMESTAMP NOT NULL COMMENT '文章發佈時間戳記',
  `canonicalUrl` VARCHAR(100) NOT NULL COMMENT '文章URL',
  `createdTime` DATETIME NOT NULL,
  `updateTime` DATETIME NOT NULL,
  `pushUp` INT NOT NULL COMMENT '文章推數',
  `pushDown` INT NOT NULL COMMENT '文章噓數',
  `content` TEXT NULL DEFAULT NULL COMMENT '文章內容',
  `commentContent` TEXT NULL DEFAULT NULL COMMENT '留言內容',
  PRIMARY KEY (`articleId`))
DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE IF NOT EXISTS `PTTDB`.`PTT_COMMENT` (
  `articleId` VARCHAR(45) NOT NULL COMMENT '文章編號',
  `commentId` VARCHAR(45) NOT NULL COMMENT '留言者ID',
  `pushTag` VARCHAR(10) COMMENT '推或噓',
  `commentContent` VARCHAR(100) NULL COMMENT '留言內容',
  `commentTime` TIMESTAMP NULL COMMENT '留言時間')
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COMMENT = 'Used to store comments in each article.';

CREATE TABLE IF NOT EXISTS `PTTDB`.`PTT_ETL_LOG` (
  `processId` VARCHAR(30) NOT NULL COMMENT 'boardName + date time',
  `etlDT` DATETIME NOT NULL COMMENT '此資訊被記錄的時間',
  `etlStatus` VARCHAR(10) NOT NULL COMMENT '兩種狀態，start及end',
  PRIMARY KEY (`processId`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COMMENT = 'Record the start time or the end time whenever the crawler program is running';

CREATE TABLE IF NOT EXISTS `PTTDB`.`PTT_ETL_DETAIL_LOG` (
  `processId` VARCHAR(30) NOT NULL COMMENT 'board_name + date time',
  `etlStatus` VARCHAR(10) NOT NULL COMMENT '訊息狀態，info、warning、error',
  `etlStatusMessage` VARCHAR(200) NOT NULL)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COMMENT = 'Record detail logs while crawler being executing';
