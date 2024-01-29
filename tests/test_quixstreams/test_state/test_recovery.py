from unittest.mock import patch


class TestChangelogManager:
    def test_add_changelog(self, changelog_manager_factory):
        changelog_manager = changelog_manager_factory()
        topic_manager = changelog_manager._topic_manager
        store_name = "my_store"
        kwargs = dict(
            topic_name="my_topic_name",
            consumer_group="my_group",
        )
        with patch.object(topic_manager, "changelog_topic") as make_changelog:
            changelog_manager.add_changelog(**kwargs, store_name=store_name)
        make_changelog.assert_called_with(**kwargs, suffix=store_name)
