# Generated by the Protocol Buffers compiler. DO NOT EDIT!
# source: snapshotter/utils/models/proto/snapshot_submission/submission.proto
# plugin: grpclib.plugin.main
import abc
import typing

import grpclib.const
import grpclib.client
if typing.TYPE_CHECKING:
    import grpclib.server

import snapshotter.utils.models.proto.snapshot_submission.submission_pb2


class SubmissionBase(abc.ABC):

    @abc.abstractmethod
    async def SubmitSnapshot(self, stream: 'grpclib.server.Stream[snapshotter.utils.models.proto.snapshot_submission.submission_pb2.SnapshotSubmission, snapshotter.utils.models.proto.snapshot_submission.submission_pb2.SubmissionResponse]') -> None:
        pass

    def __mapping__(self) -> typing.Dict[str, grpclib.const.Handler]:
        return {
            '/submission.Submission/SubmitSnapshot': grpclib.const.Handler(
                self.SubmitSnapshot,
                grpclib.const.Cardinality.STREAM_UNARY,
                snapshotter.utils.models.proto.snapshot_submission.submission_pb2.SnapshotSubmission,
                snapshotter.utils.models.proto.snapshot_submission.submission_pb2.SubmissionResponse,
            ),
        }


class SubmissionStub:

    def __init__(self, channel: grpclib.client.Channel) -> None:
        self.SubmitSnapshot = grpclib.client.StreamUnaryMethod(
            channel,
            '/submission.Submission/SubmitSnapshot',
            snapshotter.utils.models.proto.snapshot_submission.submission_pb2.SnapshotSubmission,
            snapshotter.utils.models.proto.snapshot_submission.submission_pb2.SubmissionResponse,
        )