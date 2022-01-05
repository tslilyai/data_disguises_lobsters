class DecayNotification < ApplicationMailer
  def notify(user, locator)
    @loc = locator

    mail(
      :to => user.email,
      :subject => "[#{Rails.application.name}] Your acount has been deleted"
    )
  end
end
