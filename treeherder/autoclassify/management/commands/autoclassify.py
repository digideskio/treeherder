import logging

from django.core.management.base import (BaseCommand,
                                         CommandError)

from treeherder.model.derived import JobsModel
from treeherder.model.models import (FailureLine,
                                     FailureMatch,
                                     Matcher)

logger = logging.getLogger(__name__)

# The minimum goodness of match we need to mark a particular match as the best match
AUTOCLASSIFY_CUTOFF_RATIO = 0.7
# A goodness of match after which we will not run further detectors
AUTOCLASSIFY_GOOD_ENOUGH_RATIO = 0.9


class Command(BaseCommand):
    args = '<job_guid>, <repository>'
    help = 'Mark failures on a job.'

    def handle(self, *args, **options):

        if not len(args) == 2:
            raise CommandError('2 arguments required, %s given' % len(args))
        repository, job_guid = args

        with JobsModel(repository) as jm:
            match_errors(repository, jm, job_guid)


def match_errors(repository, jm, job_guid):
    job = jm.get_job_ids_by_guid([job_guid]).get(job_guid)

    if not job:
        logger.error('autoclassify: No job for '
                     '{0} job_guid {1}'.format(repository, job_guid))
        return

    job_id = job.get("id")

    # Only try to autoclassify where we have a failure status; sometimes there can be
    # error lines even in jobs marked as passing.
    if job["result"] not in ["testfailed", "busted", "exception"]:
        return

    unmatched_failures = set(FailureLine.objects.unmatched_for_job(repository, job_guid))

    if not unmatched_failures:
        return

    all_matched = set()

    for matcher in Matcher.objects.registered_matchers():
        matches = matcher(unmatched_failures)
        for match in matches:
            match.failure_line.matches.add(
                FailureMatch(score=match.score,
                             matcher=matcher.db_object,
                             classified_failure=match.classified_failure))
            match.failure_line.save()
            logger.info("Matched failure %i with intermittent %i" %
                        (match.failure_line.id, match.classified_failure.id))
            all_matched.add(match.failure_line)
            if match.score >= AUTOCLASSIFY_GOOD_ENOUGH_RATIO:
                unmatched_failures.remove(match.failure_line)

        if not unmatched_failures:
            break

    for failure_line in all_matched:
        # TODO: store all matches
        best_match = failure_line.best_automatic_match(AUTOCLASSIFY_CUTOFF_RATIO)
        if best_match:
            failure_line.best_classification = best_match.classified_failure
            failure_line.save()

    if all_matched:
        jm.update_after_autoclassification(job_id)
