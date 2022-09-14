package io.pivotal.literx;

import io.pivotal.literx.domain.User;
import io.pivotal.literx.repository.ReactiveRepository;
import io.pivotal.literx.repository.ReactiveUserRepository;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static io.pivotal.literx.domain.User.JESSE;
import static io.pivotal.literx.domain.User.SKYLER;

/**
 * Learn how to control the demand.
 *
 * @author Sebastien Deleuze
 */
public class Part06Request {

  ReactiveRepository<User> repository = new ReactiveUserRepository();

//========================================================================================

  // TODO Create a StepVerifier that initially requests all values and expect 4 values to be received
  StepVerifier requestAllExpectFour(Flux<User> flux) {
    return StepVerifier
        .create(flux)
        .expectNextCount(4L)
        .expectComplete();
  }

//========================================================================================

  // TODO Create a StepVerifier that initially requests 1 value and expects User.SKYLER
  //  then requests another value and expects User.JESSE then stops verifying by cancelling the source
  StepVerifier requestOneExpectSkylerThenRequestOneExpectJesse(Flux<User> flux) {
    return StepVerifier
        .create(flux)
        .thenRequest(1)
        .expectNext(SKYLER)
        .thenRequest(1)
        .expectNext(JESSE)
        .thenCancel();
  }

//========================================================================================

  // TODO Return a Flux with all users stored in the repository that prints automatically logs for all Reactive Streams signals
  Flux<User> fluxWithLog() {
    return repository.findAll()
        .log();
  }

//========================================================================================

  // TODO Return a Flux with all users stored in the repository that prints "Starring:" at first,
  //  "firstname lastname" for all values and "The end!" on complete
  Flux<User> fluxWithDoOnPrintln() {
    return repository.findAll()
        .doOnSubscribe(u -> System.out.println("Starring:"))
        .doOnNext(u -> System.out.printf("%s %s%n", u.getFirstname(), u.getLastname()))
        .doOnComplete(() -> System.out.println("The end!"));
  }

}
