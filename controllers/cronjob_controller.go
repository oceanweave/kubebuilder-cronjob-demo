/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	kbatchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/reference"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/go-logr/logr"
	batchv1 "github.com/oceanweave/kubebuilder-cronjob-demo/api/v1"
	"github.com/robfig/cron"
)

var (
	scheduledTimeAnnotation = "batch.tutorial.kubebuilder.io/scheduled-at"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
	Clock
}

type realClock struct{}

func (_ realClock) Now() time.Time { return time.Now() }

// Clock knows how to get the current time.
// It can be used to fake out timing for testing.
type Clock interface {
	Now() time.Time
}

//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	/*
		我们的 CronJob controller 的基本逻辑是：
		1. 加载 CronJob
		2. 列出所有 active jobs，并更新状态
		3. 根据历史记录清理 old jobs
		4. 检查 Job 是否已被 suspended（如果被 suspended，请不要执行任何操作）
		5. 获取到下一次要 schedule 的 Job
		6. 运行新的 Job, 确定新 Job 没有超过 deadline 时间，且不会被我们 concurrency 规则 block
		7. 如果 Job 正在运行或者它应该下次运行，请重新排队
	*/
	_ = log.FromContext(ctx)
	// 在 reconcile 方法的顶部提前分配一些 key-value ,以便查找在这个 reconciler 中所有的日志
	// 创建一个新的 logr.Logger 实例，该实例会将当前处理的 CronJob 的命名空间和名称作为上下文信息附加到日志中。
	// 这样在查看日志时，可以清楚地看到哪些日志条目与哪个 CronJob 实例相关联，从而更容易进行调试和问题排查。
	log := r.Log.WithValues("cronjob", req.NamespacedName)

	// TODO(user): your logic here
	// 1. 按 namespace 加载 CrobJob
	var cronJob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		log.Error(err, "unable to fetch CronJob")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. 列出所有 active jobs，并更新状态
	var childJobs kbatchv1.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "unable to list CronJob")
		return ctrl.Result{}, err
	}
	// 查找状态为 active 的 Jobs
	var activeJobs []*kbatchv1.Job
	var successfulJobs []*kbatchv1.Job
	var failedJobs []*kbatchv1.Job
	var mostRecentTime *time.Time // 找到最后一次运行 Job，以便我们更新状态
	// 若 Job 的 succeeded 或 failed 的 Conditions 为 true，认为该 Job 已完成 finished
	isJobFinished := func(job *kbatchv1.Job) (bool, kbatchv1.JobConditionType) {
		for _, c := range job.Status.Conditions {
			if (c.Type == kbatchv1.JobComplete || c.Type == kbatchv1.JobFailed) && c.Status == corev1.ConditionTrue {
				return true, c.Type
			}

			return false, ""
		}
	}
	// 从创建 Job 时添加的 annotation 中获取到 Job 计划执行的时间
	getScheduledTimeForJob := func(job *kbatchv1.Job) (*time.Time, error) {
		timeRaw := job.Annotations[scheduledTimeAnnotation]
		if len(timeRaw) == 0 {
			return nil, nil
		}

		timeParsed, err := time.Parse(time.RFC3339, timeRaw)
		if err != nil {
			return nil, err
		}
		return &timeParsed, nil
	}

	// 根据 Job 的状态将 Job 放到不同的切片中，并获得最近一个 Job
	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "": // 正在执行
			activeJobs = append(activeJobs, &childJobs.Items[i])
		case kbatchv1.JobFailed:
			failedJobs = append(failedJobs, &childJobs.Items[i])
		case kbatchv1.JobComplete:
			successfulJobs = append(successfulJobs, &childJobs.Items[i])
		}

		// 获取 annotation 中的运行时间
		scheduledTimeForJob, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "unable to parse schedule time for child job", "job", &job)
			continue
		}
		// 获取最后执行的 Job
		if scheduledTimeForJob != nil {
			if mostRecentTime == nil {
				mostRecentTime = scheduledTimeForJob
			} else if mostRecentTime.Before(*scheduledTimeForJob) {
				mostRecentTime = scheduledTimeForJob
			}
		}
	}
	// CronJob 的 LastScheduleTime 记录 最后执行的 Job 的执行时间
	if mostRecentTime != nil {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}
	// 记录正在执行的 Job 的 Reference 信息到 cronJob.Status.Active 字段中
	cronJob.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := reference.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "unable to make reference to actibe job", "job", activeJob)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}
	// 此处为了便于调试，目前采用较高日志级别（1），后续测试完成后，请调整为较低日志级别（5），避免日志杂乱
	/*
		参考
		    PanicLevel logrus.Level = iota // 0
		    FatalLevel                     // 1
		    ErrorLevel                     // 2
		    WarnLevel                      // 3
		    InfoLevel                      // 4
		    DebugLevel                     // 5
		    TraceLevel                     // 6
	*/
	log.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs",
		len(successfulJobs), "failed jobs", len(failedJobs))
	// 更新 subresource status
	if err := r.Status().Update(ctx, &cronJob); err != nil {
		log.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}

	// 3.根据历史记录清理过期 jobs
	// 注意：这里是尽量删除，如果删除失败，我们不会为了删除让它们重新排队
	if cronJob.Spec.FailedJobsHistoryLimit != nil {
		// 排序，保证 StartTime 小的在前面
		sort.Slice(failedJobs, func(i, j int) bool {
			// 若 StartTime 为空，排在非空的前面
			if failedJobs[i].Status.StartTime == nil {
				return failedJobs[j].Status.StartTime != nil
			}
			return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
		})
		// 如果 failedJob 超出 FailedJobsHistoryLimit 就删掉
		for i, job := range failedJobs {
			if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobsHistoryLimit {
				break
			}
			// DeletePropagationForeground 父对象会被标记为删除，但会等待所有子对象被删除后才最终删除
			// DeletePropagationBackground 父对象会立即删除，但 Kubernetes 控制器会在后台继续删除所有子对象
			// DeletePropagationOrphan 父对象会被删除，但其所有子对象会被保留下来，成为孤儿对象
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old failed job", "job", job)
			} else {
				log.V(0).Info("deleted old failed job", "job", job)
			}
		}
	}
	if cronJob.Spec.SuccessfulJobsHistoryLimit != nil {
		sort.Slice(successfulJobs, func(i, j int) bool {
			if successfulJobs[i].Status.StartTime == nil {
				return successfulJobs[j].Status.StartTime != nil
			}
			return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
		})
		for i, job := range successfulJobs {
			if int32(i) >= int32(len(successfulJobs))-*cronJob.Spec.SuccessfulJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.V(0).Info("deleted old successful job", "job", job)
			}
		}
	}
	// 4. 检查 job 是否已被 suspended
	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, nil
	}
	// 5. 获取下一次要 schedule 的 Job
	getNextSchedule := func(cronJob *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
		/*
			* * * * * command to be executed
			- - - - -
			| | | | |
			| | | | +---- day of the week (0 - 7) (Sunday is both 0 and 7)
			| | | +------ month (1 - 12)
			| | +-------- day of the month (1 - 31)
			| +---------- hour (0 - 23)
			+------------ minute (0 - 59)
		*/
		// 调度周期
		sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
		// 获取上次调度时间
		var earliestTime time.Time
		if cronJob.Status.LastScheduleTime != nil {
			earliestTime = cronJob.Status.LastScheduleTime.Time
		} else {
			earliestTime = cronJob.ObjectMeta.CreationTimestamp.Time
		}
		// StartingDeadlineSeconds 是任务的执行时间长度（窗口）
		if cronJob.Spec.StartingDeadlineSeconds != nil {
			schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))
			// 若该窗口时间内，没有执行，更新上次一次执行时间
			if schedulingDeadline.After(earliestTime) {
				earliestTime = schedulingDeadline
			}
		}
		// 上次调度时间大于当前时间 now（不正确），所以 以当前时间 now 计算下次调度时间
		if earliestTime.After(now) {
			return time.Time{}, sched.Next(now), nil
		}

		// 计算错过的次数，大于 100 报错，并计算下次该调度的时间
		starts := 0
		for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
			lastMissed = t
			starts++
			if starts > 100 {
				return time.Time{}, time.Time{}, fmt.Errorf("Too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew.")
			}
		}
		return lastMissed, sched.Next(now), nil
	}
	missedRun, nextRun, err := getNextSchedule(&cronJob, r.Now())
	if err != nil {
		log.Error(err, "unable to figure out CronJob schedule")
		return ctrl.Result{}, nil
	}
	// 把下次执行 Job 的 Object 存储到 scheduledResult 变量中，知道下次需要执行的时间点，然后确定 Job 是否真的需要执行
	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Now())}
	log = log.WithValues("now", r.Now(), "next run", nextRun)

	//  result 为空，且 error 为 nil， 这表明 controller-runtime 已经成功 reconciled 了这个 object，无需进行任何重试
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Complete(r)
}
