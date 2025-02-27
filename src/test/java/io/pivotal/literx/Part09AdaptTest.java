/*
 * Copyright 2002-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pivotal.literx;

import io.pivotal.literx.domain.User;
import io.pivotal.literx.repository.ReactiveRepository;
import io.pivotal.literx.repository.ReactiveUserRepository;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;

/**
 * Learn how to adapt from/to RxJava 3 Observable/Single/Flowable and Java 8+ CompletableFuture.
 * <p>
 * Mono and Flux already implements Reactive Streams interfaces so they are natively
 * Reactive Streams compliant + there are {@link Mono#from(Publisher)} and {@link Flux#from(Publisher)}
 * factory methods.
 * <p>
 * For RxJava 3, you should not use Reactor Adapter but only RxJava 3 and Reactor Core.
 *
 * @author Sebastien Deleuze
 */
public class Part09AdaptTest {

  Part09Adapt workshop = new Part09Adapt();
  ReactiveRepository<User> repository = new ReactiveUserRepository();

//========================================================================================

  @Test
  public void adaptToFlowable() {
    Flux<User> flux = repository.findAll();
    Flowable<User> flowable = workshop.fromFluxToFlowable(flux);
    StepVerifier.create(workshop.fromFlowableToFlux(flowable))
        .expectNext(User.SKYLER, User.JESSE, User.WALTER, User.SAUL)
        .verifyComplete();
  }

//========================================================================================

  @Test
  public void adaptToObservable() {
    Flux<User> flux = repository.findAll();
    Observable<User> observable = workshop.fromFluxToObservable(flux);
    StepVerifier.create(workshop.fromObservableToFlux(observable))
        .expectNext(User.SKYLER, User.JESSE, User.WALTER, User.SAUL)
        .verifyComplete();
  }

//========================================================================================

  @Test
  public void adaptToSingle() {
    Mono<User> mono = repository.findFirst();
    Single<User> single = workshop.fromMonoToSingle(mono);
    StepVerifier.create(workshop.fromSingleToMono(single))
        .expectNext(User.SKYLER)
        .verifyComplete();
  }

//========================================================================================

  @Test
  public void adaptToCompletableFuture() {
    Mono<User> mono = repository.findFirst();
    CompletableFuture<User> future = workshop.fromMonoToCompletableFuture(mono);
    StepVerifier.create(workshop.fromCompletableFutureToMono(future))
        .expectNext(User.SKYLER)
        .verifyComplete();
  }

}
